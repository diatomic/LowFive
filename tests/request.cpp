#include "test.hpp"
#include <H5ESdevelop.h>

#include <algorithm>
#include <array>
#include <chrono>
#include <memory>
#include <thread>
#include <vector>

namespace
{
using Base = LowFive::VOLBase;
using Wrapper = Base::pass_through_t;
using Status = H5VL_request_status_t;
using Clock = std::chrono::steady_clock;

std::string fixture_filename;

// This connector actually defers native HDF5 I/O while retaining the caller's
// buffer. Its request handle and queued operation have independent lifetimes.
// Progress is explicit so timeout, cancellation and notification are deterministic.
struct Engine;
struct Dataset { hid_t id; Engine* engine; };
struct Task
{
    Dataset* dataset;
    const void* input;
    void* output;
    bool fail;
    bool cant_cancel = false;
    bool wait_error = false;
    bool cancel_error = false;
    Clock::time_point ready = Clock::now() + std::chrono::hours(1);
    Status status = H5VL_REQUEST_STATUS_IN_PROGRESS;
    hid_t error = H5I_INVALID_HID;
    H5VL_request_notify_t callback = nullptr;
    void* context = nullptr;

    ~Task() { if (error >= 0) H5Eclose_stack(error); }
    void complete()
    {
        if (status != H5VL_REQUEST_STATUS_IN_PROGRESS) return;
        const hid_t datatype = fail ? H5I_INVALID_HID : H5T_NATIVE_INT;
        const herr_t result = output
            ? H5Dread(dataset->id, datatype, H5S_ALL, H5S_ALL, H5P_DEFAULT, output)
            : H5Dwrite(dataset->id, datatype, H5S_ALL, H5S_ALL, H5P_DEFAULT, input);
        status = result < 0 ? H5VL_REQUEST_STATUS_FAIL : H5VL_REQUEST_STATUS_SUCCEED;
        if (result < 0) error = H5Eget_current_stack();
        if (callback) CHECK(callback(context, status) >= 0);
    }
};
struct Request { std::shared_ptr<Task> task; };
struct Engine
{
    std::vector<std::shared_ptr<Task>> tasks;
    int live_handles = 0;
    int submissions = 0;
    int fail_submission = -1;
    int fail_operation = -1;
    bool fail_after_submission = false;
    bool ready_on_submit = false;
    bool immediate_notify = false;
    int notify_calls = 0;
    int fail_notify = -1;
    int free_calls = 0;
    int fail_free = -1;

    herr_t submit(Dataset* dataset, const void* input, void* output, void** request)
    {
        const int index = submissions++;
        if (index == fail_submission && !fail_after_submission) return -1;
        auto task = std::make_shared<Task>();
        task->dataset = dataset;
        task->input = input;
        task->output = output;
        task->fail = index == fail_operation;
        if (ready_on_submit) task->ready = Clock::now();
        if (!request)
        {
            task->complete();
            return task->status == H5VL_REQUEST_STATUS_FAIL ? -1 : 0;
        }
        CHECK(*request == nullptr);
        tasks.push_back(task);
        *request = new Request{task};
        ++live_handles;
        return index == fail_submission ? -1 : 0;
    }
    void release(Request* request) { --live_handles; delete request; }
    void complete_all() { for (const auto& task : tasks) task->complete(); }
};

herr_t under_read(size_t count, void* objects[], hid_t[], hid_t[], hid_t[], hid_t, void* buffers[], void** request)
{
    CHECK(count == 1);
    auto* dataset = static_cast<Dataset*>(objects[0]);
    return dataset->engine->submit(dataset, nullptr, buffers[0], request);
}
herr_t under_write(size_t count, void* objects[], hid_t[], hid_t[], hid_t[], hid_t, const void* buffers[], void** request)
{
    CHECK(count == 1);
    auto* dataset = static_cast<Dataset*>(objects[0]);
    return dataset->engine->submit(dataset, buffers[0], nullptr, request);
}

herr_t under_produce(void* object, H5VL_optional_args_t* args, hid_t, void** request)
{
    if (args->op_type != 701) return -1;
    auto* dataset = static_cast<Dataset*>(object);
    return dataset->engine->submit(dataset, args->args, nullptr, request);
}
herr_t under_object_produce(void* object, const H5VL_loc_params_t*, H5VL_optional_args_t* args,
                            hid_t dxpl, void** request)
{
    return under_produce(object, args, dxpl, request);
}
herr_t under_opt_query(void*, H5VL_subclass_t, int, uint64_t* flags)
{
    *flags = 0;
    return 0;
}
herr_t under_wait(void* object, uint64_t timeout, Status* status)
{
    auto& task = *static_cast<Request*>(object)->task;
    if (task.wait_error)
    {
        task.wait_error = false;
        return -1;
    }
    if (task.status == H5VL_REQUEST_STATUS_IN_PROGRESS)
    {
        if (timeout == H5ES_WAIT_FOREVER) task.complete();
        else if (task.ready <= Clock::now()) task.complete();
        else if (timeout)
        {
            const auto duration = std::chrono::nanoseconds(std::min<uint64_t>(timeout, 1000000));
            std::this_thread::sleep_for(duration);
            if (task.ready <= Clock::now()) task.complete();
        }
    }
    *status = task.status;
    return 0;
}
herr_t under_cancel(void* object, Status* status)
{
    auto& task = *static_cast<Request*>(object)->task;
    if (task.cancel_error)
    {
        task.cancel_error = false;
        return -1;
    }
    if (task.status != H5VL_REQUEST_STATUS_IN_PROGRESS) *status = task.status;
    else if (task.cant_cancel)
    {
        task.cant_cancel = false;
        *status = H5VL_REQUEST_STATUS_CANT_CANCEL;
    }
    else *status = task.status = H5VL_REQUEST_STATUS_CANCELED;
    return 0;
}
herr_t under_notify(void* object, H5VL_request_notify_t callback, void* context)
{
    auto* request = static_cast<Request*>(object);
    auto task = request->task;
    auto& engine = *task->dataset->engine;
    if (engine.notify_calls++ == engine.fail_notify) return -1;
    task->callback = callback;
    task->context = context;
    engine.release(request);
    if (task->status != H5VL_REQUEST_STATUS_IN_PROGRESS) return callback(context, task->status);
    if (engine.immediate_notify) task->complete();
    return 0;
}
herr_t under_specific(void* object, H5VL_request_specific_args_t* args)
{
    auto& task = *static_cast<Request*>(object)->task;
    if (args->op_type == H5VL_REQUEST_GET_ERR_STACK)
    {
        const hid_t stack = H5Ecreate_stack();
        if (stack < 0) return -1;
        if (task.error >= 0 && H5Eappend_stack(stack, task.error, false) < 0)
        {
            H5Eclose_stack(stack);
            return -1;
        }
        args->args.get_err_stack.err_stack_id = stack;
        return 0;
    }
    if (args->op_type == H5VL_REQUEST_GET_EXEC_TIME)
    {
        if (args->args.get_exec_time.exec_ts) *args->args.get_exec_time.exec_ts = 100;
        if (args->args.get_exec_time.exec_time) *args->args.get_exec_time.exec_time = 25;
        return 0;
    }
    return -1;
}
herr_t under_optional(void* object, H5VL_optional_args_t* args)
{
    // A connector-specific progress operation, not an echo of input arguments.
    if (args->op_type != 700) return -1;
    static_cast<Request*>(object)->task->complete();
    return 0;
}
herr_t under_free(void* object)
{
    auto* request = static_cast<Request*>(object);
    auto& engine = *request->task->dataset->engine;
    if (engine.free_calls++ == engine.fail_free) return -1;
    engine.release(request);
    return 0;
}

H5VL_class_t under_class()
{
    H5VL_class_t connector{};
    connector.version = H5VL_VERSION;
    connector.value = static_cast<H5VL_class_value_t>(512);
    connector.name = "lowfive-deferred-native-test";
    connector.cap_flags = H5VL_CAP_FLAG_ASYNC;
    connector.dataset_cls.read = under_read;
    connector.dataset_cls.write = under_write;
    connector.object_cls.optional = under_object_produce;
    connector.introspect_cls.opt_query = under_opt_query;
    connector.optional = under_produce;
    connector.request_cls = {under_wait, under_notify, under_cancel, under_specific, under_optional, under_free};
    return connector;
}

// Give public async APIs real dataset IDs, so HDF5 records the operation's
// source/API metadata and can subsequently return H5ES_err_info_t.
struct DeferredVOL : Base
{
    std::array<Dataset, 3>* datasets = nullptr;
    void* file_open(const char*, unsigned, hid_t, hid_t, void**) override { return datasets; }
    herr_t file_close(void*, hid_t, void**) override { return 0; }
    void* dataset_open(void*, const H5VL_loc_params_t*, const char* name, hid_t, hid_t, void**) override
    {
        for (size_t index = 0; index < datasets->size(); ++index)
            if (std::string(name) == "values" + std::to_string(index))
                return &(*datasets)[index];
        return nullptr;
    }
    herr_t dataset_close(void*, hid_t, void**) override { return 0; }
};

struct Fixture
{
    Engine engine;
    test::Id file;
    test::Id scalar;
    std::array<test::Id, 3> native;
    std::array<Dataset, 3> datasets;
    std::array<Wrapper*, 3> wrappers{};
    Base& vol;
    hid_t connector;
    hid_t underlying;

    Fixture(): file(H5Fcreate(fixture_filename.c_str(), H5F_ACC_TRUNC, H5P_DEFAULT, H5P_DEFAULT), H5Fclose),
        scalar(H5Screate(H5S_SCALAR), H5Sclose), vol(*new DeferredVOL), connector(vol.register_plugin())
    {
        const auto cls = under_class();
        underlying = H5VLregister_connector(&cls, H5P_DEFAULT);
        CHECK(underlying >= 0);
        CHECK(H5Iinc_ref(underlying) >= 0);
        CHECK(H5Idec_ref(vol.info->under_vol_id) >= 0);
        vol.info->under_vol_id = underlying;
        static_cast<DeferredVOL&>(vol).datasets = &datasets;
        for (size_t index = 0; index < native.size(); ++index)
        {
            const std::string name = "values" + std::to_string(index);
            native[index].id = H5Dcreate2(file, name.c_str(), H5T_NATIVE_INT, scalar,
                                         H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);
            native[index].close = H5Dclose;
            CHECK(native[index].id >= 0);
            datasets[index] = Dataset{native[index], &engine};
            wrappers[index] = new Wrapper(&datasets[index], underlying, &vol);
        }
        CHECK(H5VLstart_lib_state() >= 0);
    }
    ~Fixture()
    {
        H5VLfinish_lib_state();
        for (auto* wrapper : wrappers) Wrapper::destroy_wrapper(wrapper);
        engine.tasks.clear();
        H5VLunregister_connector(connector);
        H5VLunregister_connector(underlying);
        for (auto& dataset : native) dataset.reset();
        scalar.reset();
        file.reset();
        std::remove(fixture_filename.c_str());
    }
    void* write(size_t count, const std::array<int, 3>& values, herr_t* result = nullptr)
    {
        std::array<void*, 3> objects{wrappers[0], wrappers[1], wrappers[2]};
        std::array<hid_t, 3> types{H5T_NATIVE_INT, H5T_NATIVE_INT, H5T_NATIVE_INT};
        std::array<hid_t, 3> spaces{H5S_ALL, H5S_ALL, H5S_ALL};
        std::array<const void*, 3> buffers{&values[0], &values[1], &values[2]};
        void* request = nullptr;
        const herr_t status = H5VLdataset_write(count, objects.data(), connector, types.data(), spaces.data(),
                                                spaces.data(), H5P_DATASET_XFER_DEFAULT, buffers.data(), &request);
        if (result) *result = status;
        else CHECK(status >= 0);
        return request;
    }
    void* read(size_t count, std::array<int, 3>& values)
    {
        std::array<void*, 3> objects{wrappers[0], wrappers[1], wrappers[2]};
        std::array<hid_t, 3> types{H5T_NATIVE_INT, H5T_NATIVE_INT, H5T_NATIVE_INT};
        std::array<hid_t, 3> spaces{H5S_ALL, H5S_ALL, H5S_ALL};
        std::array<void*, 3> buffers{&values[0], &values[1], &values[2]};
        void* request = nullptr;
        CHECK(H5VLdataset_read(count, objects.data(), connector, types.data(), spaces.data(), spaces.data(),
                               H5P_DATASET_XFER_DEFAULT, buffers.data(), &request) >= 0);
        return request;
    }
    Status wait(void* request, uint64_t timeout = H5ES_WAIT_FOREVER)
    {
        Status status = H5VL_REQUEST_STATUS_IN_PROGRESS;
        CHECK(H5VLrequest_wait(request, connector, timeout, &status) >= 0);
        return status;
    }
    int value(size_t index)
    {
        int result = -1;
        CHECK(H5Dread(native[index], H5T_NATIVE_INT, H5S_ALL, H5S_ALL, H5P_DEFAULT, &result) >= 0);
        return result;
    }
    void release(void* request) { CHECK(H5VLrequest_free(request, connector) >= 0); }
};

struct Notified { int calls = 0; Status status = H5VL_REQUEST_STATUS_IN_PROGRESS; };
herr_t notification(void* argument, Status status)
{
    auto& result = *static_cast<Notified*>(argument);
    ++result.calls;
    result.status = status;
    return 0;
}

void lifecycle(bool multiple)
{
    Fixture fixture;
    const size_t count = multiple ? 3 : 1;
    const std::array<int, 3> values{11, 22, 33};
    const int references = H5Iget_ref(fixture.underlying);
    void* request = fixture.write(count, values);
    CHECK(request != nullptr);
    CHECK(fixture.wait(request, H5ES_WAIT_NONE) == H5VL_REQUEST_STATUS_IN_PROGRESS);
    CHECK(fixture.wait(request, 1) == H5VL_REQUEST_STATUS_IN_PROGRESS);
    CHECK(fixture.value(0) == 0);
    if (multiple)
    {
        fixture.engine.tasks.front()->ready = Clock::now();
        CHECK(fixture.wait(request, H5ES_WAIT_NONE) == H5VL_REQUEST_STATUS_IN_PROGRESS);
        CHECK(fixture.value(0) == 11);
        CHECK(fixture.value(1) == 0);
    }
    CHECK(fixture.wait(request) == H5VL_REQUEST_STATUS_SUCCEED);
    uint64_t timestamp = 0, duration = 0;
    H5VL_request_specific_args_t timing{};
    timing.op_type = H5VL_REQUEST_GET_EXEC_TIME;
    timing.args.get_exec_time.exec_ts = &timestamp;
    timing.args.get_exec_time.exec_time = &duration;
    CHECK(H5VLrequest_specific(request, fixture.connector, &timing) >= 0);
    CHECK(timestamp == 100 && duration == 25);
    fixture.release(request);
    CHECK(fixture.engine.live_handles == 0);
    CHECK(H5Iget_ref(fixture.underlying) == references);
    std::array<int, 3> actual{-1, -1, -1};
    request = fixture.read(count, actual);
    CHECK(actual[0] == -1);
    CHECK(fixture.wait(request) == H5VL_REQUEST_STATUS_SUCCEED);
    for (size_t index = 0; index < count; ++index) CHECK(actual[index] == values[index]);
    fixture.release(request);
    CHECK(fixture.engine.live_handles == 0);
}

void cancellation()
{
    Fixture fixture;
    const std::array<int, 3> values{41, 42, 43};
    void* request = fixture.write(3, values);
    fixture.engine.tasks[1]->cant_cancel = true;
    Status status;
    CHECK(H5VLrequest_cancel(request, fixture.connector, &status) >= 0);
    CHECK(status == H5VL_REQUEST_STATUS_CANT_CANCEL);
    CHECK(fixture.value(0) == 0 && fixture.value(1) == 0);
    CHECK(H5VLrequest_cancel(request, fixture.connector, &status) >= 0);
    CHECK(status == H5VL_REQUEST_STATUS_CANCELED);
    fixture.release(request);
    fixture.engine.complete_all();
    CHECK(fixture.value(0) == 0 && fixture.value(1) == 0 && fixture.value(2) == 0);
    CHECK(fixture.engine.live_handles == 0);
    request = fixture.write(3, values);
    fixture.release(request); // Explicit free detaches tracking, not the I/O itself.
    fixture.engine.complete_all();
    CHECK(fixture.value(0) == 41 && fixture.value(1) == 42 && fixture.value(2) == 43);
    const std::array<int, 3> canceled{44, 45, 46};
    request = fixture.write(1, canceled);
    CHECK(H5VLrequest_cancel(request, fixture.connector, &status) >= 0);
    CHECK(status == H5VL_REQUEST_STATUS_CANCELED);
    fixture.release(request);
    fixture.engine.complete_all();
    CHECK(fixture.value(0) == 41 && fixture.engine.live_handles == 0);
}

int event_completed(const H5ES_op_info_t* info, H5ES_status_t status, hid_t, void* argument)
{
    CHECK(status == H5ES_STATUS_SUCCEED);
    CHECK(info->op_exec_ts == 100 && info->op_exec_time == 25);
    ++*static_cast<int*>(argument);
    return 0;
}
void event_set(bool fail)
{
    Fixture fixture;
    const std::array<int, 3> values{51, 52, 53};
    if (fail) fixture.engine.fail_operation = 0;
    test::Id fapl(H5Pcreate(H5P_FILE_ACCESS), H5Pclose);
    CHECK(H5Pset_vol(fapl, fixture.connector, fixture.vol.info) >= 0);
    test::Id file(H5Fopen("deferred", H5F_ACC_RDWR, fapl), H5Fclose);
    std::array<test::Id, 3> datasets;
    std::array<hid_t, 3> ids;
    for (size_t index = 0; index < datasets.size(); ++index)
    {
        const auto name = "values" + std::to_string(index);
        datasets[index].id = H5Dopen2(file, name.c_str(), H5P_DEFAULT);
        datasets[index].close = H5Dclose;
        CHECK(datasets[index].id >= 0);
        ids[index] = datasets[index];
    }
    test::Id events(H5EScreate(), H5ESclose);
    int completed = 0;
    if (!fail) CHECK(H5ESregister_complete_func(events, event_completed, &completed) >= 0);
    std::array<hid_t, 3> types{H5T_NATIVE_INT, H5T_NATIVE_INT, H5T_NATIVE_INT};
    std::array<hid_t, 3> spaces{H5S_ALL, H5S_ALL, H5S_ALL};
    std::array<const void*, 3> buffers{&values[0], &values[1], &values[2]};
    CHECK(H5Dwrite_multi_async(ids.size(), ids.data(), types.data(), spaces.data(), spaces.data(),
                               H5P_DEFAULT, buffers.data(), events) >= 0);
    if (fail)
    {
        fixture.engine.tasks[0]->ready = Clock::now();
        size_t pending = 0;
        bool failed = true;
        CHECK(H5ESwait(events, H5ES_WAIT_NONE, &pending, &failed) >= 0);
        CHECK(pending == 1 && !failed);
    }
    size_t pending = 99;
    bool failed = false;
    CHECK(H5ESwait(events, H5ES_WAIT_FOREVER, &pending, &failed) >= 0);
    CHECK(pending == 0 && failed == fail);
    if (fail)
    {
        CHECK(fixture.value(0) == 0 && fixture.value(1) == 52 && fixture.value(2) == 53);
        H5ES_err_info_t error{};
        size_t cleared = 0;
        CHECK(H5ESget_err_info(events, 1, &error, &cleared) >= 0);
        CHECK(cleared == 1);
        CHECK(H5Eget_num(error.err_stack_id) > 0);
        CHECK(H5ESfree_err_info(1, &error) >= 0);
    }
    else CHECK(completed == 1);
    CHECK(fixture.engine.live_handles == 0);
}

void submission_failure(bool produces_request)
{
    Fixture fixture;
    const std::array<int, 3> values{61, 62, 63};
    fixture.engine.fail_submission = 1;
    fixture.engine.fail_after_submission = produces_request;
    fixture.engine.ready_on_submit = true;
    herr_t result = 0;
    CHECK(fixture.write(3, values, &result) == nullptr);
    CHECK(result < 0);
    CHECK(fixture.engine.submissions == 2);
    CHECK(fixture.engine.live_handles == 0);
    CHECK(fixture.value(0) == 61);
    CHECK(fixture.value(1) == (produces_request ? 62 : 0));
    CHECK(fixture.value(2) == 0);
}

void forwarding_errors()
{
    Fixture fixture;
    const std::array<int, 3> values{71, 72, 73};
    void* request = fixture.write(3, values);
    H5VL_optional_args_t unsupported{700, nullptr};
    CHECK(H5VLrequest_optional(request, fixture.connector, &unsupported) < 0);
    CHECK(fixture.value(0) == 0 && fixture.engine.live_handles == 3);
    fixture.engine.tasks[0]->wait_error = true;
    Status status;
    CHECK(H5VLrequest_wait(request, fixture.connector, H5ES_WAIT_NONE, &status) < 0);
    CHECK(status == H5VL_REQUEST_STATUS_IN_PROGRESS);
    CHECK(fixture.wait(request) == H5VL_REQUEST_STATUS_SUCCEED);
    fixture.engine.fail_free = 1;
    CHECK(H5VLrequest_free(request, fixture.connector) < 0);
    CHECK(fixture.engine.live_handles == 2);
    fixture.release(request);
    CHECK(fixture.engine.live_handles == 0);
    request = fixture.write(1, values);
    H5VL_optional_args_t progress{700, nullptr};
    CHECK(H5VLrequest_optional(request, fixture.connector, &progress) >= 0);
    CHECK(fixture.value(0) == 71);
    fixture.release(request);
    H5VL_optional_args_t produce{701, const_cast<int*>(&values[0])};
    H5VL_loc_params_t location{};
    location.obj_type = H5I_DATASET;
    location.type = H5VL_OBJECT_BY_SELF;
    request = nullptr;
    CHECK(H5VLobject_optional(fixture.wrappers[0], &location, fixture.connector, &produce,
                               H5P_DATASET_XFER_DEFAULT, &request) >= 0);
    CHECK(request != nullptr && fixture.engine.live_handles == 1);
    CHECK(fixture.wait(request) == H5VL_REQUEST_STATUS_SUCCEED);
    fixture.release(request);
    request = nullptr;
    CHECK(H5VLoptional(fixture.wrappers[0], fixture.connector, &produce,
                       H5P_DATASET_XFER_DEFAULT, &request) >= 0);
    CHECK(request != nullptr && fixture.engine.live_handles == 1);
    CHECK(fixture.wait(request) == H5VL_REQUEST_STATUS_SUCCEED);
    fixture.release(request);
    CHECK(fixture.engine.live_handles == 0 && fixture.value(0) == 71);
}

void bounded_cleanup()
{
    Fixture fixture;
    const std::array<int, 3> values{74, 75, 76};
    void* request = fixture.write(1, values);
    Base::drain_request(static_cast<Wrapper*>(request));
    CHECK(fixture.engine.live_handles == 0);
    CHECK(fixture.engine.tasks[0]->status == H5VL_REQUEST_STATUS_CANCELED);
    CHECK(fixture.value(0) == 0);
    request = fixture.write(1, values);
    fixture.engine.tasks[1]->wait_error = true;
    fixture.engine.tasks[1]->cancel_error = true;
    Base::drain_request(static_cast<Wrapper*>(request));
    CHECK(fixture.engine.live_handles == 1);
    CHECK(H5Eget_num(H5E_DEFAULT) > 0);
    CHECK(fixture.value(0) == 0);
    // The broken-under-VOL path retained ownership instead of detaching I/O.
    // Once the underlying connector recovers, the same request remains usable.
    CHECK(fixture.wait(request) == H5VL_REQUEST_STATUS_SUCCEED);
    fixture.release(request);
    CHECK(fixture.engine.live_handles == 0 && fixture.value(0) == 74);
}

herr_t throwing_notification(void*, Status)
{
    throw std::runtime_error("notification callback failure");
}

void singleton_notification()
{
    Fixture fixture;
    const std::array<int, 3> values{91, 92, 93};
    void* request = fixture.write(1, values);
    Notified result;
    CHECK(H5VLrequest_notify(request, fixture.connector, notification, &result) >= 0);
    CHECK(result.calls == 0 && fixture.value(0) == 0);
    fixture.engine.complete_all();
    CHECK(result.calls == 1 && result.status == H5VL_REQUEST_STATUS_SUCCEED);
    CHECK(fixture.value(0) == 91 && fixture.engine.live_handles == 0);
}

void notifications(const std::string& name)
{
    Fixture fixture;
    const std::array<int, 3> values{81, 82, 83};
    const int references = H5Iget_ref(fixture.underlying);
    if (name == "notify-error") fixture.engine.fail_operation = 0;
    void* request = fixture.write(3, values);
    Notified result;
    if (name == "notify-immediate" || name == "notify-exception") fixture.engine.immediate_notify = true;
    if (name == "notify-partial" || name == "notify-retry") fixture.engine.fail_notify = 1;
    const auto callback = name == "notify-exception" ? throwing_notification : notification;
    const herr_t registered = H5VLrequest_notify(request, fixture.connector, callback, &result);
    if (name == "notify-exception")
    {
        CHECK(registered < 0);
        CHECK(H5Eget_num(H5E_DEFAULT) > 0);
        CHECK(fixture.wait(request) == H5VL_REQUEST_STATUS_SUCCEED);
        fixture.release(request);
        CHECK(fixture.engine.live_handles == 0);
        CHECK(fixture.value(0) == 81 && fixture.value(1) == 82 && fixture.value(2) == 83);
        return;
    }
    if (name == "notify-partial" || name == "notify-retry")
    {
        CHECK(registered < 0);
        CHECK(result.calls == 0);
        fixture.engine.tasks[0]->complete();
        CHECK(result.calls == 0);
        if (name == "notify-retry")
        {
            CHECK(H5VLrequest_notify(request, fixture.connector, notification, &result) >= 0);
            fixture.engine.complete_all();
            CHECK(result.calls == 1 && result.status == H5VL_REQUEST_STATUS_SUCCEED);
        }
        else
        {
            CHECK(fixture.wait(request) == H5VL_REQUEST_STATUS_SUCCEED);
            CHECK(result.calls == 0);
            fixture.release(request);
        }
    }
    else
    {
        CHECK(registered >= 0);
        if (name != "notify-immediate")
        {
            CHECK(result.calls == 0 && fixture.value(0) == 0);
            fixture.engine.tasks[2]->complete();
            CHECK(result.calls == 0 && fixture.value(2) == 83);
            fixture.engine.tasks[0]->complete();
            CHECK(result.calls == 0);
            fixture.engine.tasks[1]->complete();
        }
        CHECK(result.calls == 1);
        CHECK(result.status == (name == "notify-error" ? H5VL_REQUEST_STATUS_FAIL : H5VL_REQUEST_STATUS_SUCCEED));
    }
    CHECK(fixture.engine.live_handles == 0);
    CHECK(H5Iget_ref(fixture.underlying) == references);
    CHECK(fixture.value(0) == (name == "notify-error" ? 0 : 81));
    CHECK(fixture.value(1) == 82 && fixture.value(2) == 83);
}

struct ContextVOL : Base
{
    int mode = 0;
    int acquired = 0;
    int released = 0;
    bool fail_release = false;
    herr_t get_wrap_ctx(void*, void** context) override
    {
        *context = new int(17);
        ++acquired;
        if (mode == 1) return -1;
        if (mode == 2) throw std::runtime_error("context failure");
        return 0;
    }
    herr_t free_wrap_ctx(void* context) override
    {
        if (fail_release) { fail_release = false; return -1; }
        delete static_cast<int*>(context);
        ++released;
        return 0;
    }
};
void wrapping_failures()
{
    auto& vol = *new ContextVOL;
    const hid_t connector = vol.register_plugin();
    const hid_t underlying = H5VL_NATIVE;
    auto* object = new Wrapper(reinterpret_cast<void*>(1), underlying, &vol);
    const int references = H5Iget_ref(underlying);
    for (int mode : {1, 2})
    {
        vol.mode = mode;
        void* context = reinterpret_cast<void*>(1);
        CHECK(Base::connector.wrap_cls.get_wrap_ctx(object, &context) < 0);
        CHECK(context == nullptr);
        CHECK(vol.acquired == vol.released);
        CHECK(H5Iget_ref(underlying) == references);
    }
    void* context = reinterpret_cast<void*>(1);
    CHECK(Base::connector.wrap_cls.get_wrap_ctx(nullptr, &context) < 0);
    CHECK(context == nullptr);
    const int acquired = vol.acquired;
    object->under_vol_id = H5I_INVALID_HID;
    CHECK(Base::connector.wrap_cls.get_wrap_ctx(object, &context) < 0);
    CHECK(context == nullptr && vol.acquired == acquired);
    object->under_vol_id = underlying;
    vol.mode = 0;
    CHECK(Base::connector.wrap_cls.get_wrap_ctx(object, &context) >= 0);
    CHECK(context != nullptr && H5Iget_ref(underlying) == references + 1);
    vol.fail_release = true;
    CHECK(Base::connector.wrap_cls.free_wrap_ctx(context) < 0);
    CHECK(H5Iget_ref(underlying) == references + 1);
    CHECK(Base::connector.wrap_cls.free_wrap_ctx(context) >= 0);
    CHECK(H5Iget_ref(underlying) == references);
    CHECK(vol.acquired == vol.released);
    CHECK(Base::connector.wrap_cls.wrap_object(reinterpret_cast<void*>(1), H5I_DATASET, nullptr) == nullptr);
    CHECK(Base::connector.wrap_cls.unwrap_object(nullptr) == nullptr);
    CHECK(Base::connector.wrap_cls.free_wrap_ctx(nullptr) < 0);
    Wrapper::destroy_wrapper(object);
    CHECK(H5VLunregister_connector(connector) >= 0);
}
}

int main(int argc, char** argv)
{
    return test::run(argc, argv, [](const std::string& name)
    {
        fixture_filename = "request-" + name + ".h5";
        if (name == "singleton") lifecycle(false);
        else if (name == "multiple") lifecycle(true);
        else if (name == "cancel-free") cancellation();
        else if (name == "event-success") event_set(false);
        else if (name == "event-failure") event_set(true);
        else if (name == "submission-failure") submission_failure(false);
        else if (name == "submission-request-failure") submission_failure(true);
        else if (name == "forwarding-errors") forwarding_errors();
        else if (name == "bounded-cleanup") bounded_cleanup();
        else if (name == "notify-singleton") singleton_notification();
        else if (name == "wrap-failures") wrapping_failures();
        else notifications(name);
    });
}
