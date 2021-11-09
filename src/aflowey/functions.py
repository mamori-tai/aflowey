import asyncio
import functools
import inspect
from typing import Any, Callable, List, Union, Iterable, Optional, cast, Type

from loguru import logger

from aflowey import F
from aflowey.types import Function


def async_wrap(func: Function) -> F:
    """wrap a function into a coroutine function
    Args:
        func: callable
    Returns:
        F instance
    """

    @functools.wraps(func)
    async def wrapper(*a: Any, **kw: Any) -> Any:
        return func(*a, **kw)

    return F(wrapper)


def log(log_str: str = "", print_arg: bool = False) -> Any:
    """utility function to log between steps, printing argument if needed"""

    async def wrapped(last_result: Any) -> Any:
        if log_str:
            logger.info(log_str)
        if print_arg:  # pragma: no cover
            logger.info(last_result)
        return last_result

    return wrapped


flog = log


def partial(func: Function, *args: Any, **kwargs: Any) -> F:
    """make a partial function of the given func and ensure
    it will work in an async context
    """
    return F(cast(Callable[[], Any], functools.partial(func, *args, **kwargs)))


p = apartial = partial


def f1(func: Function, extractor: Optional[Function] = None) -> F:
    """wraps a one argument function (with arity 1) and allows
    to add an extractor to work on the input argument.
    """

    is_coroutine = asyncio.iscoroutinefunction(func)

    @functools.wraps(func)
    def wrapped(arg1: Any) -> Any:
        value = arg1 if extractor is None else extractor(arg1)
        return func(value)

    wrapped_fn = async_wrap(wrapped) if is_coroutine else F(wrapped)
    wrapped_fn.__F1__ = True  # type: ignore

    return wrapped_fn


F1 = f1


def f0(func: Function) -> F:
    """create a new function from a 0 arity function (takes 0 args). The new
    function takes exactly one argument and does not pass it to the wrapped
    function. It allows using a 0 arity function in a flow relatively easily.
    """
    is_coroutine = asyncio.iscoroutinefunction(func)

    @functools.wraps(func)
    def wrapped(arg1: Any) -> Any:
        return func()

    wrapped_fn = async_wrap(wrapped) if is_coroutine else F(wrapped)
    wrapped_fn.__F0__ = True  # type: ignore

    return wrapped_fn


F0 = f0


def may_fail(func: Function) -> Function:
    """simply for readability"""
    return func


breaker = erratic = may_fail


def spread_args(func: Function) -> F:
    """create a function which takes an iterable of args
    and spread it into the given function"""

    is_coroutine = asyncio.iscoroutinefunction(func)

    @functools.wraps(func)
    def wrapped(args: Iterable[Any]) -> Any:
        # if to much args, slice the args to given length
        return func(*args)

    wrapped_fn = async_wrap(wrapped) if is_coroutine else F(wrapped)
    return wrapped_fn


spread = spread_args


def spread_kwargs(func: Function) -> F:
    """create a function which takes a mapping of kwargs
    and spread it into the given function"""

    is_coroutine = asyncio.iscoroutinefunction(func)

    @functools.wraps(func)
    def wrapped(**kwargs: Any) -> Any:
        arg_spec = inspect.getfullargspec(func)
        args = set(arg_spec.args + arg_spec.kwonlyargs) - set(["self"])
        new_kwargs = {key: kwargs[key] for key in args}
        return func(**new_kwargs)

    wrapped_fn = async_wrap(wrapped) if is_coroutine else F(wrapped)
    return wrapped_fn


spread_kw = spread_kwargs


def lift(f: Function, lift_op: Function = map) -> F:  # type: ignore[assignment]
    return p(lift_op, f)


def ensure_f(func: Function) -> F:
    """wrap the given function into a F instance"""
    if not isinstance(func, F):
        return F(func)
    return func


def make_impure(func: Union[Function, F]) -> F:
    """tags the given function as impure, i.e. consume an argument
    but does not create new one
    """
    # automatically create new function when the function
    # is a bound method and has no other input args
    func = ensure_f(func)
    func.__side_effect__ = True  # type: ignore
    return func


def side_effect(*func: Union[Function, F]) -> Union[List[F], F]:
    """
    take an array of function and tag it as side effects
    function
    """
    if len(func) == 1:
        return make_impure(func[0])
    return [make_impure(fu) for fu in func]


imp = impure = side_effect


def ensure_callable(x: Union[Any, Function]) -> Function:
    """ensure a given args is a callable by returning a new callable if not"""
    if not callable(x):

        def wrapped(*args: Any, **kwargs: Any) -> Any:
            return x

        return cast(Function, wrapped)
    return cast(Function, x)


def named(func: Union[Function, F], name: str) -> F:
    """tags a function as a named function"""
    if not callable(func):
        func = ensure_callable(func)
    func = ensure_f(func)
    func.__named__ = name  # type: ignore
    return func


def identity(x: Any) -> Any:
    return x


# helper functions
def is_f(func: Any) -> bool:
    return isinstance(func, F)


def is_f0(func: Function) -> bool:
    return hasattr(func, "__F0__")


def is_f1(func: Function) -> bool:
    return hasattr(func, "__F1__")


def is_side_effect(func: Function) -> bool:
    return hasattr(func, "__side_effect__")


def get_name(func: Function) -> str:
    if hasattr(func, "__named__"):
        return func.__named__  # type: ignore
    return ""
