#pragma once

#include <string>
#include <tuple>
#include <map>
#include <vector>
#include <memory>

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

    void*           create(size_t id, size_t constructor_id, server* self) const;

    inline void     call(size_t id, server* self) const;
    inline void     call_mem_fn(size_t id, server* self) const;

    const class_proxy_base&
                    proxy(size_t id) const          { return *classes_[id]; }

    std::vector<std::unique_ptr<FunctionBase>>      functions_;     // NB: identifiable only by their index
    std::vector<std::unique_ptr<class_proxy_base>>  classes_;
};

/* Functions */
struct server::module::FunctionBase
{
    virtual void    call(server* self)  =0;
};

template<class R, class... Args>
struct server::module::Function: public FunctionBase
{
                    Function(R (*f)(Args...)):
                        f_(f)                       {}

    void    call(server* self) override
    {
        R res = call_impl(typename detail::gens<sizeof...(Args)>::type(), self->receive<Args...>());
        self->send(res);
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

    void    call(server* self) override
    {
        call_impl(typename detail::gens<sizeof...(Args)>::type(), self->receive<Args...>());
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
call(size_t id, server* self) const
{ functions_[id]->call(self); }


/* MemberFunctions */
struct server::module::MemberFunctionBase
{
    virtual void    call(size_t obj_id, server* self)  =0;
};

template<class C, class R, class... Args>
struct server::module::MemberFunction: public MemberFunctionBase
{
                    MemberFunction(R (C::*f)(Args...)):
                        f_(f)                       {}

    void    call(size_t obj_id, server* self) override
    {
        C* x = (C*) self->objects_[obj_id];

        R res = call_impl(x, typename detail::gens<sizeof...(Args)>::type(), self->receive<Args...>());
        self->send(res);
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

    void    call(size_t obj_id, server* self) override
    {
        C* x = (C*) self->objects_[obj_id];

        call_impl(x, typename detail::gens<sizeof...(Args)>::type(), self->receive<Args...>());
    }

    private:
        template<int... S>
        void    call_impl(C* x, detail::seq<S...>, std::tuple<Args...> params)
        {
            (x->*f_)(std::get<S>(params)...);
        }

        void    (C::*f_)(Args...);
};

/* Constructor */
struct server::module::ConstructorBase
{
    virtual void*   create(server* self)    =0;
};

template<class C, class... Args>
struct server::module::Constructor: public ConstructorBase
{
    void*           create(server* self) override
    {
        return create_impl(typename detail::gens<sizeof...(Args)>::type(), self->receive<Args...>());
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
    virtual void*   create(size_t constructor_id, server* self) const =0;
    virtual void    call(size_t id, size_t obj_id, server* self) const =0;
    virtual void    destroy(void* o) const =0;
};

template<class C>
struct server::module::class_proxy: public class_proxy_base
{
    void*           create(size_t id, server* self) const override      { return constructors_[id]->create(self); }

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

    void            call(size_t id, size_t obj_id, server* self) const override
    {
        functions_[id]->call(obj_id, self);
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
create(size_t class_id, size_t constructor_id, server* self) const
{ return classes_[class_id]->create(constructor_id, self); }


}   // namespace rpc
}   // namespace LowFive
