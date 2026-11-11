#include <lowfive/vol-base.hpp>

#include <algorithm>
#include <chrono>
#include <condition_variable>
#include <limits>
#include <mutex>
#include <stdexcept>
#include <utility>
#include <vector>

namespace
{
using Status = H5VL_request_status_t;
using Clock = std::chrono::steady_clock;

bool terminal(Status status)
{
    return status == H5VL_REQUEST_STATUS_SUCCEED || status == H5VL_REQUEST_STATUS_FAIL ||
           status == H5VL_REQUEST_STATUS_CANCELED;
}

uint64_t remaining(uint64_t timeout, Clock::time_point start)
{
    if (timeout == H5ES_WAIT_FOREVER || timeout == H5ES_WAIT_NONE)
        return timeout;
    const auto elapsed = std::chrono::duration_cast<std::chrono::nanoseconds>(Clock::now() - start).count();
    return static_cast<uint64_t>(elapsed) >= timeout ? 0 : timeout - static_cast<uint64_t>(elapsed);
}

herr_t unsupported(const char* message)
{
    H5Epush2(H5E_DEFAULT, __FILE__, "LowFive request", __LINE__, H5E_ERR_CLS,
             H5E_VOL, H5E_UNSUPPORTED, "%s", message);
    return -1;
}

herr_t invoke_notify(H5VL_request_notify_t callback, void* context, Status status) noexcept
{
    try { return callback(context, status); }
    catch (...)
    {
        H5Epush2(H5E_DEFAULT, __FILE__, "LowFive request notification", __LINE__, H5E_ERR_CLS,
                 H5E_VOL, H5E_CALLBACK, "%s", "C++ exception in request notification");
        return -1;
    }
}
}

struct LowFive::VOLBase::request_state_t
{
    struct Entry
    {
        void* object = nullptr;
        hid_t connector;
        Status status = H5VL_REQUEST_STATUS_SUCCEED;
        bool notified = false;

        explicit Entry(hid_t id): connector(id)
        {
            if (H5Iinc_ref(connector) < 0)
                throw std::runtime_error("cannot retain underlying request connector");
        }
        Entry(const Entry&) = delete;
        Entry& operator=(const Entry&) = delete;
        Entry(Entry&& other) noexcept:
            object(other.object), connector(std::exchange(other.connector, H5I_INVALID_HID)),
            status(other.status), notified(other.notified) {}
        ~Entry()
        {
            if (connector >= 0)
            {
                const hid_t stack = H5Eget_current_stack();
                H5Idec_ref(connector);
                H5Eset_current_stack(stack);
            }
        }
    };

    std::vector<Entry> entries;
    std::mutex mutex;
    std::condition_variable completed;
    H5VL_request_notify_t callback = nullptr;
    void* context = nullptr;
    bool armed = false;
    bool called = false;

    Status summary(bool cancel = false) const
    {
        bool failed = false, canceled = false, pending = false, cant_cancel = false;
        for (const auto& entry : entries)
        {
            failed |= entry.status == H5VL_REQUEST_STATUS_FAIL;
            canceled |= entry.status == H5VL_REQUEST_STATUS_CANCELED;
            pending |= !terminal(entry.status);
            cant_cancel |= entry.status == H5VL_REQUEST_STATUS_CANT_CANCEL;
        }
        // Never report terminal failure while another operation still uses a caller buffer.
        if (pending)
            return cancel && cant_cancel ? H5VL_REQUEST_STATUS_CANT_CANCEL : H5VL_REQUEST_STATUS_IN_PROGRESS;
        if (failed) return H5VL_REQUEST_STATUS_FAIL;
        if (canceled) return H5VL_REQUEST_STATUS_CANCELED;
        return H5VL_REQUEST_STATUS_SUCCEED;
    }

    herr_t deliver()
    {
        H5VL_request_notify_t function;
        void* argument;
        Status status;
        {
            std::lock_guard<std::mutex> lock(mutex);
            status = summary();
            if (!armed || called || !terminal(status)) return 0;
            called = true;
            function = callback;
            argument = context;
        }
        return invoke_notify(function, argument, status);
    }
};

namespace
{
using Wrapper = LowFive::VOLBase::pass_through_t;
using State = LowFive::VOLBase::request_state_t;

std::shared_ptr<State> state_for(Wrapper* request)
{
    if (!request->request_state)
    {
        auto state = std::make_shared<State>();
        state->entries.emplace_back(request->under_vol_id);
        state->entries.front().object = request->under_object;
        state->entries.front().status = H5VL_REQUEST_STATUS_IN_PROGRESS;
        request->request_state = std::move(state);
        request->under_object = nullptr;
    }
    return request->request_state;
}

struct Notification
{
    std::shared_ptr<State> state;
    size_t index;
};

herr_t notified(void* argument, Status status) noexcept
{
    std::unique_ptr<Notification> notification(static_cast<Notification*>(argument));
    auto state = notification->state;
    try
    {
        {
            std::lock_guard<std::mutex> lock(state->mutex);
            state->entries[notification->index].status = status;
        }
        state->completed.notify_all();
        return state->deliver();
    }
    catch (...)
    {
        H5Epush2(H5E_DEFAULT, __FILE__, "LowFive aggregate notification", __LINE__, H5E_ERR_CLS,
                 H5E_VOL, H5E_CALLBACK, "%s", "C++ exception in aggregate request notification");
        return -1;
    }
}
}

LowFive::VOLBase::pass_through_t*
LowFive::VOLBase::create_request(size_t count, void* objects[])
{
    auto* first = static_cast<pass_through_t*>(objects[0]);
    std::unique_ptr<pass_through_t, void (*)(pass_through_t*)> request(
        first->create(nullptr), pass_through_t::destroy_wrapper);
    if (count == 1) return request.release();
    auto state = std::make_shared<request_state_t>();
    state->entries.reserve(count);
    for (size_t index = 0; index < count; ++index)
        state->entries.emplace_back(static_cast<pass_through_t*>(objects[index])->under_vol_id);
    request->request_state = std::move(state);
    return request.release();
}

void
LowFive::VOLBase::set_request(pass_through_t* request, size_t index, void* underlying)
{
    if (!request->request_state)
    {
        request->under_object = underlying;
        return;
    }
    auto& entry = request->request_state->entries[index];
    entry.object = underlying;
    entry.status = underlying ? H5VL_REQUEST_STATUS_IN_PROGRESS : H5VL_REQUEST_STATUS_SUCCEED;
}

herr_t
LowFive::VOLBase::_request_wait(void* argument, uint64_t timeout, Status* status)
{
    if (!argument || !status) return -1;
    auto* request = static_cast<pass_through_t*>(argument);
    // HDF5 event sets query timing/error information after terminal wait and
    // release requests separately with request_free. A terminal wait is not free.
    if (!request->request_state)
    {
        if (request->under_object)
            return H5VLrequest_wait(request->under_object, request->under_vol_id, timeout, status);
        *status = H5VL_REQUEST_STATUS_SUCCEED;
        return 0;
    }

    auto state = request->request_state;
    const auto start = Clock::now();
    herr_t result = 0;
    for (auto& entry : state->entries)
    {
        if (entry.notified)
        {
            std::unique_lock<std::mutex> lock(state->mutex);
            const auto done = [&entry] { return terminal(entry.status); };
            const auto budget = remaining(timeout, start);
            if (budget == H5ES_WAIT_FOREVER)
                state->completed.wait(lock, done);
            else if (budget != H5ES_WAIT_NONE)
                state->completed.wait_for(lock, std::chrono::nanoseconds(
                    std::min<uint64_t>(budget, std::numeric_limits<int64_t>::max())), done);
        }
        else if (entry.object && !terminal(entry.status))
        {
            Status child_status = H5VL_REQUEST_STATUS_IN_PROGRESS;
            const herr_t child_result = H5VLrequest_wait(entry.object, entry.connector,
                                                        remaining(timeout, start), &child_status);
            if (child_result < 0) result = child_result;
            else
            {
                std::lock_guard<std::mutex> lock(state->mutex);
                entry.status = child_status;
            }
        }
    }
    std::lock_guard<std::mutex> lock(state->mutex);
    *status = state->summary();
    return result;
}

herr_t
LowFive::VOLBase::_request_cancel(void* argument, Status* status)
{
    if (!argument || !status) return -1;
    auto* request = static_cast<pass_through_t*>(argument);
    if (!request->request_state)
    {
        if (request->under_object)
            return H5VLrequest_cancel(request->under_object, request->under_vol_id, status);
        *status = H5VL_REQUEST_STATUS_SUCCEED;
        return 0;
    }
    auto state = request->request_state;
    herr_t result = 0;
    for (auto& entry : state->entries)
        if (!entry.notified && entry.object && !terminal(entry.status))
        {
            Status child_status = H5VL_REQUEST_STATUS_IN_PROGRESS;
            const herr_t child_result = H5VLrequest_cancel(entry.object, entry.connector, &child_status);
            if (child_result < 0) result = child_result;
            else
            {
                std::lock_guard<std::mutex> lock(state->mutex);
                entry.status = child_status;
            }
        }
    std::lock_guard<std::mutex> lock(state->mutex);
    *status = state->summary(true);
    return result;
}

herr_t
LowFive::VOLBase::_request_notify(void* argument, H5VL_request_notify_t callback, void* context)
{
    if (!argument || !callback) return -1;
    auto* request = static_cast<pass_through_t*>(argument);
    auto state = state_for(request);
    // Allocate every callback context before transferring even one request.
    std::vector<std::unique_ptr<Notification>> notifications(state->entries.size());
    for (size_t index = 0; index < state->entries.size(); ++index)
        if (state->entries[index].object)
            notifications[index] = std::make_unique<Notification>(Notification{state, index});
    {
        std::lock_guard<std::mutex> lock(state->mutex);
        state->callback = callback;
        state->context = context;
        state->armed = false;
    }
    for (size_t index = 0; index < state->entries.size(); ++index)
    {
        auto& entry = state->entries[index];
        if (!entry.object) continue;
        // A successful registration consumes the underlying request. On a
        // partial failure the wrapper remains usable: wait observes callbacks
        // already transferred, and notify/free can retry the remaining handles.
        auto* notification = notifications[index].release();
        const herr_t result = H5VLrequest_notify(entry.object, entry.connector, notified, notification);
        if (result < 0)
        {
            delete notification;
            return result;
        }
        entry.object = nullptr;
        entry.notified = true;
    }
    {
        std::lock_guard<std::mutex> lock(state->mutex);
        state->armed = true;
    }
    const herr_t result = state->deliver();
    if (result >= 0) pass_through_t::destroy_wrapper(request);
    return result;
}

herr_t
LowFive::VOLBase::_request_specific(void* argument, H5VL_request_specific_args_t* args)
{
    if (!argument || !args) return -1;
    auto* request = static_cast<pass_through_t*>(argument);
    if (!request->request_state)
        return H5VLrequest_specific(request->under_object, request->under_vol_id, args);
    auto state = request->request_state;
    if (state->entries.size() == 1 && state->entries.front().object)
        return H5VLrequest_specific(state->entries.front().object, state->entries.front().connector, args);
    if (args->op_type == H5VL_REQUEST_GET_ERR_STACK)
    {
        const hid_t stack = H5Ecreate_stack();
        if (stack < 0) return -1;
        for (const auto& entry : state->entries)
            if (entry.object && entry.status == H5VL_REQUEST_STATUS_FAIL)
            {
                H5VL_request_specific_args_t child{};
                child.op_type = H5VL_REQUEST_GET_ERR_STACK;
                child.args.get_err_stack.err_stack_id = H5I_INVALID_HID;
                if (H5VLrequest_specific(entry.object, entry.connector, &child) < 0)
                {
                    H5Eclose_stack(stack);
                    return -1;
                }
                const herr_t appended = H5Eappend_stack(stack, child.args.get_err_stack.err_stack_id, false);
                H5Eclose_stack(child.args.get_err_stack.err_stack_id);
                if (appended < 0)
                {
                    H5Eclose_stack(stack);
                    return -1;
                }
            }
        args->args.get_err_stack.err_stack_id = stack;
        return 0;
    }
    if (args->op_type == H5VL_REQUEST_GET_EXEC_TIME)
    {
        uint64_t first = UINT64_MAX, last = 0;
        for (const auto& entry : state->entries)
            if (entry.object)
            {
                uint64_t timestamp = 0, duration = 0;
                H5VL_request_specific_args_t child{};
                child.op_type = H5VL_REQUEST_GET_EXEC_TIME;
                child.args.get_exec_time.exec_ts = &timestamp;
                child.args.get_exec_time.exec_time = &duration;
                if (H5VLrequest_specific(entry.object, entry.connector, &child) < 0) return -1;
                first = std::min(first, timestamp);
                last = std::max(last, timestamp + std::min(duration, UINT64_MAX - timestamp));
            }
        if (args->args.get_exec_time.exec_ts) *args->args.get_exec_time.exec_ts = first;
        if (args->args.get_exec_time.exec_time)
            *args->args.get_exec_time.exec_time = first == UINT64_MAX ? 0 : last - first;
        return 0;
    }
    return unsupported("unknown aggregate request-specific operation");
}

herr_t
LowFive::VOLBase::_request_optional(void* argument, H5VL_optional_args_t* args)
{
    if (!argument || !args) return -1;
    auto* request = static_cast<pass_through_t*>(argument);
    if (!request->request_state)
        return H5VLrequest_optional(request->under_object, request->under_vol_id, args);
    auto state = request->request_state;
    if (state->entries.size() == 1 && state->entries.front().object)
        return H5VLrequest_optional(state->entries.front().object, state->entries.front().connector, args);
    return unsupported("connector-specific request operations have no aggregate interpretation");
}

herr_t
LowFive::VOLBase::_request_free(void* argument)
{
    if (!argument) return -1;
    auto* request = static_cast<pass_through_t*>(argument);
    if (!request->request_state)
    {
        const herr_t result = request->under_object
            ? H5VLrequest_free(request->under_object, request->under_vol_id) : 0;
        if (result >= 0) pass_through_t::destroy_wrapper(request);
        return result;
    }
    for (auto& entry : request->request_state->entries)
        if (entry.object)
        {
            const herr_t result = H5VLrequest_free(entry.object, entry.connector);
            if (result < 0) return result;
            entry.object = nullptr;
        }
    pass_through_t::destroy_wrapper(request);
    return 0;
}

void
LowFive::VOLBase::drain_request(pass_through_t* request)
{
    Status status = H5VL_REQUEST_STATUS_IN_PROGRESS;
    // A failed submission cannot publish an event-set request. Poll completed
    // work before cancellation: waiting forever here could block indefinitely
    // on an otherwise conforming connector that requires external progress.
    const herr_t waited = _request_wait(request, H5ES_WAIT_NONE, &status);
    if (waited < 0 || !terminal(status))
        _request_cancel(request, &status);
    if (terminal(status))
        _request_free(request);
    else
    {
        // A broken underlying VOL that fails both wait and cancellation cannot
        // safely release a caller buffer. Retain its handle and connector refs,
        // reporting cleanup failure rather than detaching it or looping forever.
        H5Epush2(H5E_DEFAULT, __FILE__, "LowFive request cleanup", __LINE__, H5E_ERR_CLS,
                 H5E_VOL, H5E_CANTRELEASE, "%s", "underlying I/O could not be drained or canceled");
    }
}
