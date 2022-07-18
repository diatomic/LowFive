#pragma once

#include <string>
#include <tuple>
#include <map>
#include <vector>
#include <memory>

#include <lowfive/metadata/serialization.hpp>

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

    template<class R, class... Args>
    void            function(std::string name, R (*f)(Args...))
    {
        functions_.emplace_back(new Function<R, Args...>(f));
    }

    template<class C>
    class_proxy<C>& class_(std::string name);

    void*           create(diy::MemoryBuffer& in, diy::MemoryBuffer& out);
    void            destroy(diy::MemoryBuffer& in);

    inline void     call(diy::MemoryBuffer& in, diy::MemoryBuffer& out) const;
    inline void     call_mem_fn(diy::MemoryBuffer& in, diy::MemoryBuffer& out) const;

    const class_proxy_base&
                    proxy(size_t id) const          { return *classes_[id]; }

    // load a tuple
    template<class... Args> struct load_impl;
    template<class... Args>
    std::tuple<Args...>
                    load(diy::MemoryBuffer& in)     { return load_impl<Args...>()(in, this); }

    std::vector<std::unique_ptr<FunctionBase>>      functions_;     // NB: identifiable only by their index
    std::vector<std::unique_ptr<class_proxy_base>>  classes_;

    struct ObjectWithClass
    {
        void*   obj;
        size_t  cls;
    };
    std::vector<ObjectWithClass>        objects_;
};

// load_impl
template<class T, class... Args>
struct server::module::load_impl<T, Args...>
{
    std::tuple<T, Args...>  operator()(diy::MemoryBuffer& in, module* self) const
    {
        T x;
        diy::load(in, x);
        return std::tuple_cat(std::make_tuple(x), load_impl<Args...>()(in, self));
    }
};

// T*
template<class T, class... Args>
struct server::module::load_impl<T*, Args...>
{
    std::tuple<T*, Args...> operator()(diy::MemoryBuffer& in, module* self) const
    {
        size_t obj_id;
        diy::load(in, obj_id);
        return std::tuple_cat(std::make_tuple(static_cast<T*>(self->objects_[obj_id].obj)), load_impl<Args...>()(in, self));
    }
};

// T&
template<class T, class... Args>
struct server::module::load_impl<T&, Args...>
{
    std::tuple<T&, Args...> operator()(diy::MemoryBuffer& in, module* self) const
    {
        size_t obj_id;
        diy::load(in, obj_id);
        T& x = *static_cast<T*>(self->objects_[obj_id].obj);
        return std::tuple_cat(std::tuple<T&>(x), load_impl<Args...>()(in,self));
    }
};

template<>
struct server::module::load_impl<>
{
    std::tuple<>            operator()(diy::MemoryBuffer& in, module* self) const      { return std::tuple<>(); }
};



/* Functions */
struct server::module::FunctionBase
{
    virtual void    call(diy::MemoryBuffer& in, diy::MemoryBuffer& out, module* self)  =0;
};

template<class R, class... Args>
struct server::module::Function: public FunctionBase
{
                    Function(R (*f)(Args...)):
                        f_(f)                       {}

    void    call(diy::MemoryBuffer& in, diy::MemoryBuffer& out, module* self) override
    {
        R res = call_impl(typename detail::gens<sizeof...(Args)>::type(), self->load<Args...>(in));
        diy::save(out, res);
    }

    private:
        template<int... S>
        R       call_impl(detail::seq<S...>, std::tuple<Args...> params)
        {
            return (*f_)(std::get<S>(params)...);
        }

        R   (*f_)(Args...);
};

template<class... Args>
struct server::module::Function<void,Args...>: public FunctionBase
{
                    Function(void (*f)(Args...)):
                        f_(f)                       {}

    void    call(diy::MemoryBuffer& in, diy::MemoryBuffer& out, module* self) override
    {
        call_impl(typename detail::gens<sizeof...(Args)>::type(), self->load<Args...>(in));
    }

    private:
        template<int... S>
        void    call_impl(detail::seq<S...>, std::tuple<Args...> params)
        {
            (*f_)(std::get<S>(params)...);
        }

        void    (*f_)(Args...);
};


void
server::module::
call(diy::MemoryBuffer& in, diy::MemoryBuffer& out) const
{
    size_t id;
    diy::load(in, id);

    functions_[id]->call(in, out, this);
}


/* MemberFunctions */
struct server::module::MemberFunctionBase
{
    virtual void    call(size_t obj_id, diy::MemoryBuffer& in, diy::MemoryBuffer& out, module* self)  =0;
};

template<class C, class R, class... Args>
struct server::module::MemberFunction: public MemberFunctionBase
{
                    MemberFunction(R (C::*f)(Args...)):
                        f_(f)                       {}

    void    call(size_t obj_id, diy::MemoryBuffer& in, diy::MemoryBuffer& out, module* self) override
    {
        C* x = (C*) self->objects_[obj_id].obj;

        R res = call_impl(x, typename detail::gens<sizeof...(Args)>::type(), self->load<Args...>(in));
        diy::save(out, res);
    }

    private:
        template<int... S>
        R       call_impl(C* x, detail::seq<S...>, std::tuple<Args...> params)
        {
            return (x->*f_)(std::get<S>(params)...);
        }

        R   (C::*f_)(Args...);
};

template<class C, class... Args>
struct server::module::MemberFunction<C,void,Args...>: public MemberFunctionBase
{
                    MemberFunction(void (C::*f)(Args...)):
                        f_(f)                       {}

    void    call(size_t obj_id, diy::MemoryBuffer& in, diy::MemoryBuffer& /* out */, module* self) override
    {
        C* x = (C*) self->objects_[obj_id].obj;

        call_impl(x, typename detail::gens<sizeof...(Args)>::type(), self->load<Args...>(in));
    }

    private:
        template<int... S>
        void    call_impl(C* x, detail::seq<S...>, std::tuple<Args...> params)
        {
            (x->*f_)(std::get<S>(params)...);
        }

        void    (C::*f_)(Args...);
};

void
server::module::
call_mem_fn(diy::MemoryBuffer& in, diy::MemoryBuffer& out) const
{
    size_t obj_id;
    diy::load(in, obj_id);

    size_t class_id = objects_[obj_id].cls;
    auto& cp = m_.proxy(class_id);
    size_t fn_id;
    diy::load(in, fn_id);

    cp.call(fn_id, obj_id, in, out);
}


/* Constructor */
struct server::module::ConstructorBase
{
    virtual void*   create(diy::MemoryBuffer& in, module* self)    =0;
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
};

template<class C>
struct server::module::class_proxy: public class_proxy_base
{
    void*           create(size_t id, diy::MemoryBuffer& in, module* self) const override      { return constructors_[id]->create(in, self); }

    template<class... Args>
    class_proxy&    constructor()
    {
        constructors_.emplace_back(new Constructor<C, Args...>);
        return *this;
    }


    template<class R, class... Args>
    class_proxy&    function(std::string name, R (C::*f)(Args...))
    {
        functions_.emplace_back(new MemberFunction<C, R, Args...>(f));
        return *this;
    }

    void            call(size_t id, size_t obj_id, diy::MemoryBuffer& in, diy::MemoryBuffer& out, module* self) const override
    {
        functions_[id]->call(obj_id, in, out, self);
    }

    void            destroy(void* o) const override     { delete static_cast<C*>(o); }

    std::vector<std::unique_ptr<MemberFunctionBase>>    functions_;
    std::vector<std::unique_ptr<ConstructorBase>>       constructors_;
};


template<class C>
server::module::class_proxy<C>&
server::module::
class_(std::string name)
{
    class_proxy<C>* x = new class_proxy<C>;
    classes_.emplace_back(x);
    return *x;
}

void*
server::module::
create(diy::MemoryBuffer& in, diy::MemoryBuffer& out)
{
    size_t class_id, constructor_id;
    diy::load(in, class_id);
    diy::load(in, constructor_id);

    objects_.emplace_back(classes_[class_id]->create(constructor_id, in, this), class_id);

    size_t obj_id = objects_.size() - 1;
    diy::save(out, obj_id);
}

void
server::module::
destroy(diy::MemoryBuffer& in)
{
    size_t obj_id;
    diy::load(in, obj_id);
    m_.proxy(obj_id).destroy(objects_[obj_id].obj);

    objects_[obj_id].obj = 0;
}


}   // namespace rpc
}   // namespace LowFive
