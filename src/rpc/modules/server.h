#pragma once

#include <string>
#include <tuple>
#include <map>
#include <vector>
#include <memory>

#include "../../metadata/serialization.hpp"

#include "../util.h"

namespace LowFive
{

namespace rpc
{

struct server::module
{
    struct FunctionBase;
    template<class R, class... Args> struct Function;

    struct class_proxy_base;
    template<class C>
    struct class_proxy;

    struct MemberFunctionBase;
    template<class C, class R, class... Args> struct MemberFunction;

    struct ConstructorBase;
    template<class C, class... Args> struct Constructor;

    template<class F>
    void            function(std::string name, F f)
    {
        function(name, to_function(f));
    }

    template<class R, class... Args>
    void        function(std::string name, R (*f)(Args...))
    {
        function(name, to_function(f));
    }

    template<class R, class... Args>
    void            function(std::string name, std::function<R(Args...)> f)
    {
        functions_.emplace_back(new Function<R, Args...>(name, f));
    }

    template<class C>
    class_proxy<C>& class_(std::string name);

    inline void     create(diy::MemoryBuffer& in, diy::MemoryBuffer& out);
    inline void     destroy(diy::MemoryBuffer& in);

    inline void     call(diy::MemoryBuffer& in, diy::MemoryBuffer& out);
    inline void     call_mem_fn(diy::MemoryBuffer& in, diy::MemoryBuffer& out);

    const class_proxy_base&
                    proxy(size_t id) const          { return *classes_[id]; }

    template<class C>
    size_t          class_id() const                { return class_hashes_.at(hash_class<C>()); }

    // load a tuple
    template<class... Args>
    std::tuple<Args...>
                    load(diy::MemoryBuffer& in)     { return std::tuple<Args...> { load_value<Args>(in)... }; }
                                                    // NB: must use braced initializer list, instead of a function call to make_tuple,
                                                    //     to ensure the correct order

    template<class T>
    T               load_value(diy::MemoryBuffer& in);

    std::vector<std::unique_ptr<FunctionBase>>      functions_;     // NB: identifiable only by their index
    std::vector<std::unique_ptr<class_proxy_base>>  classes_;
    std::map<size_t, size_t>                        class_hashes_;

    struct ObjectWithClass
    {
        void*   obj;
        size_t  cls;
        bool    own;
    };
    std::vector<ObjectWithClass>        objects_;
};

template<class T>
T
server::module::
load_value(diy::MemoryBuffer& in)
{
    if constexpr (std::is_pointer_v<T>)
    {
        size_t obj_id;
        diy::load(in, obj_id);
        return static_cast<T>(objects_[obj_id].obj);
    } else if constexpr (std::is_reference_v<T>)
    {
        size_t obj_id;
        diy::load(in, obj_id);
        return *static_cast<std::add_pointer_t<T>>(objects_[obj_id].obj);
    } else
    {
        T x;
        diy::load(in, x);
        return x;
    }
}

/* Functions */
struct server::module::FunctionBase
{
                    FunctionBase(std::string name):
                        name_(name)                         {}
    virtual void    call(diy::MemoryBuffer& in, diy::MemoryBuffer& out, module* self)  =0;
    virtual         ~FunctionBase() {}

    std::string     name_;
};

template<class R, class... Args>
struct server::module::Function: public FunctionBase
{
                    Function(std::string name, std::function<R(Args...)> f):
                        FunctionBase(name),
                        f_(f)                       {}

    void    call(diy::MemoryBuffer& in, diy::MemoryBuffer& out, module* self) override
    {
        auto log = get_logger();
        log->trace("Called function {}", name_);
        R res = call_impl(typename detail::gens<sizeof...(Args)>::type(), self->load<Args...>(in));
        diy::save(out, res);
    }

    private:
        template<int... S>
        R       call_impl(detail::seq<S...>, std::tuple<Args...> params)
        {
            return f_(std::get<S>(params)...);
        }

        std::function<R(Args...)> f_;
};

// specialization for returning pointers
template<class R, class... Args>
struct server::module::Function<R*, Args...>: public FunctionBase
{
                    Function(std::string name, std::function<R*(Args...)> f):
                        FunctionBase(name),
                        f_(f)                       {}

    void    call(diy::MemoryBuffer& in, diy::MemoryBuffer& out, module* self) override
    {
        auto log = get_logger();
        log->trace("Called function {}", name_);

        R* res = call_impl(typename detail::gens<sizeof...(Args)>::type(), self->load<Args...>(in));

        size_t cls = self->class_id<R>();

        self->objects_.emplace_back(ObjectWithClass { res, cls, false });
        size_t obj_id = self->objects_.size() - 1;

        diy::save(out, cls);
        diy::save(out, obj_id);
    }

    private:
        template<int... S>
        R*       call_impl(detail::seq<S...>, std::tuple<Args...> params)
        {
            return f_(std::get<S>(params)...);
        }

        std::function<R*(Args...)> f_;
};

template<class... Args>
struct server::module::Function<void,Args...>: public FunctionBase
{
                    Function(std::string name, std::function<void(Args...)> f):
                        FunctionBase(name),
                        f_(f)                       {}

    void    call(diy::MemoryBuffer& in, diy::MemoryBuffer& out, module* self) override
    {
        auto log = get_logger();
        log->trace("Called function {}", name_);

        call_impl(typename detail::gens<sizeof...(Args)>::type(), self->load<Args...>(in));
    }

    private:
        template<int... S>
        void    call_impl(detail::seq<S...>, std::tuple<Args...> params)
        {
            f_(std::get<S>(params)...);
        }

        std::function<void(Args...)> f_;
};


void
server::module::
call(diy::MemoryBuffer& in, diy::MemoryBuffer& out)
{
    size_t id;
    diy::load(in, id);

    functions_[id]->call(in, out, this);
}


/* MemberFunctions */
struct server::module::MemberFunctionBase
{
                    MemberFunctionBase(std::string name):
                        name_(name)         {}
    virtual void    call(size_t obj_id, diy::MemoryBuffer& in, diy::MemoryBuffer& out, module* self)  =0;
    virtual         ~MemberFunctionBase()   {}

    std::string     name_;
};

template<class C, class R, class... Args>
struct server::module::MemberFunction: public MemberFunctionBase
{
                    MemberFunction(std::string name, std::function<R(C*, Args...)> f):
                        MemberFunctionBase(name),
                        f_(f)                       {}

    void    call(size_t obj_id, diy::MemoryBuffer& in, diy::MemoryBuffer& out, module* self) override
    {
        auto log = get_logger();
        log->trace("Called member function {}", name_);

        C* x = (C*) self->objects_[obj_id].obj;

        R res = call_impl(x, typename detail::gens<sizeof...(Args)>::type(), self->load<Args...>(in));
        diy::save(out, res);
    }

    private:
        template<int... S>
        R       call_impl(C* x, detail::seq<S...>, std::tuple<Args...> params)
        {
            return f_(x, std::get<S>(params)...);
        }

        std::function<R(C*, Args...)> f_;
};

// specialization for returning pointers
template<class C, class R, class... Args>
struct server::module::MemberFunction<C, R*, Args...>: public MemberFunctionBase
{
                    MemberFunction(std::string name, std::function<R*(C*, Args...)> f):
                        MemberFunctionBase(name),
                        f_(f)                       {}

    void    call(size_t obj_id, diy::MemoryBuffer& in, diy::MemoryBuffer& out, module* self) override
    {
        auto log = get_logger();
        log->trace("Called member function {}", name_);

        C* x = (C*) self->objects_[obj_id].obj;

        R* res = call_impl(x, typename detail::gens<sizeof...(Args)>::type(), self->load<Args...>(in));

        size_t cls = self->class_id<R>();

        self->objects_.emplace_back(ObjectWithClass { res, cls, false });
        size_t res_obj_id = self->objects_.size() - 1;

        diy::save(out, cls);
        diy::save(out, res_obj_id);
    }

    private:
        template<int... S>
        R*      call_impl(C* x, detail::seq<S...>, std::tuple<Args...> params)
        {
            return f_(x, std::get<S>(params)...);
        }

        std::function<R*(C*, Args...)> f_;
};

template<class C, class... Args>
struct server::module::MemberFunction<C,void,Args...>: public MemberFunctionBase
{
                    MemberFunction(std::string name, std::function<void(C*, Args...)> f):
                        MemberFunctionBase(name),
                        f_(f)                       {}

    void    call(size_t obj_id, diy::MemoryBuffer& in, diy::MemoryBuffer& /* out */, module* self) override
    {
        auto log = get_logger();
        log->trace("Called member function {}", name_);

        C* x = (C*) self->objects_[obj_id].obj;

        call_impl(x, typename detail::gens<sizeof...(Args)>::type(), self->load<Args...>(in));
    }

    private:
        template<int... S>
        void    call_impl(C* x, detail::seq<S...>, std::tuple<Args...> params)
        {
            f_(x, std::get<S>(params)...);
        }

        std::function<void(C*, Args...)> f_;
};

/* Constructor */
struct server::module::ConstructorBase
{
    virtual void*   create(diy::MemoryBuffer& in, module* self)    =0;
    virtual         ~ConstructorBase()  {}
};

template<class C, class... Args>
struct server::module::Constructor: public ConstructorBase
{
    void*           create(diy::MemoryBuffer& in, module* self) override
    {
        return create_impl(typename detail::gens<sizeof...(Args)>::type(), self->load<Args...>(in));
    }

    private:
        template<int... S>
        void*   create_impl(detail::seq<S...>, std::tuple<Args...> params)
        {
            return new C(std::get<S>(params)...);
        }
};


/* class_proxy */
struct server::module::class_proxy_base
{
    virtual void*   create(size_t constructor_id, diy::MemoryBuffer& in, module* self) const =0;
    virtual void    call(size_t id, size_t obj_id, diy::MemoryBuffer& in, diy::MemoryBuffer& out, module* self) const =0;
    virtual void    destroy(void* o) const =0;
    virtual         ~class_proxy_base()     {}
};

template<class C>
struct server::module::class_proxy: public class_proxy_base
{
                    class_proxy():
                        class_hash_(hash_class<C>())                                        {}

    void*           create(size_t id, diy::MemoryBuffer& in, module* self) const override   { return constructors_[id]->create(in, self); }

    template<class... Args>
    class_proxy&    constructor()
    {
        constructors_.emplace_back(new Constructor<C, Args...>);
        return *this;
    }


    template<class R, class... Args>
    class_proxy&    function(std::string name, std::function<R(C*, Args...)> f)
    {
        functions_.emplace_back(new MemberFunction<C, R, Args...>(name, f));
        return *this;
    }

    template<class R, class... Args>
    class_proxy&    function(std::string name, R (C::*f)(Args...))
    {
        return function(name, to_function(f));
    }

    template<class F>
    class_proxy&    function(std::string name, F f)
    {
        return function(name, to_function(f));
    }

    void            call(size_t id, size_t obj_id, diy::MemoryBuffer& in, diy::MemoryBuffer& out, module* self) const override
    {
        functions_[id]->call(obj_id, in, out, self);
    }

    void            destroy(void* o) const override     { delete static_cast<C*>(o); }

    size_t          hash() const                        { return class_hash_; }

    std::vector<std::unique_ptr<MemberFunctionBase>>    functions_;
    std::vector<std::unique_ptr<ConstructorBase>>       constructors_;
    size_t                                              class_hash_;
};


template<class C>
server::module::class_proxy<C>&
server::module::
class_(std::string name)
{
    class_proxy<C>* x = new class_proxy<C>;
    classes_.emplace_back(x);

    class_hashes_.emplace(x->hash(), classes_.size() - 1);

    return *x;
}

void
server::module::
call_mem_fn(diy::MemoryBuffer& in, diy::MemoryBuffer& out)
{
    size_t obj_id;
    diy::load(in, obj_id);

    size_t cls = objects_[obj_id].cls;
    auto& cp = proxy(cls);
    size_t fn_id;
    diy::load(in, fn_id);

    cp.call(fn_id, obj_id, in, out, this);
}

void
server::module::
create(diy::MemoryBuffer& in, diy::MemoryBuffer& out)
{
    size_t cls, constructor_id;
    diy::load(in, cls);
    diy::load(in, constructor_id);

    objects_.emplace_back(ObjectWithClass { classes_[cls]->create(constructor_id, in, this), cls, true });

    size_t obj_id = objects_.size() - 1;
    diy::save(out, cls);        // redundant, but matches the reading on the client, designed to support returning objects from function calls
    diy::save(out, obj_id);
}

void
server::module::
destroy(diy::MemoryBuffer& in)
{
    size_t obj_id;
    diy::load(in, obj_id);

    if (!objects_[obj_id].own)
    {
        auto log = get_logger();
        log->error("Destroying an object we don't own: {}", obj_id);
        throw std::runtime_error("Destroying an object we don't own");
    }

    proxy(obj_id).destroy(objects_[obj_id].obj);
    objects_[obj_id].obj = 0;
}


}   // namespace rpc
}   // namespace LowFive
