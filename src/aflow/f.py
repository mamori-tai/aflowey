import functools
from inspect import iscoroutinefunction, iscoroutine, getfullargspec
from typing import Any, Callable, List

from loguru import logger


def identity(x: Any) -> Any:
    return x


class F:
    """tiny wrapper around a function

    Args:
        func: callable

    """

    def __init__(self, func: Callable):
        self.func = func
        functools.update_wrapper(self, func)

    def __rshift__(self, other: Callable) -> "F":
        """equivalent of decoration"""
        return F(other(self.func))

    def __gt__(self, other: Callable) -> "F":
        """function composition"""

        #@functools.wraps(other)
        def wrapped(*args, **kwargs):
            return other(self.func(*args, **kwargs))

        return F(wrapped)

    def __call__(self, *args, **kwargs) -> Any:
        """ simply call the inner function"""
        return self.func(*args, **kwargs)

    def __repr__(self) -> str:
        return f"<F instance: {repr(self.func)}"


def async_wrap(func: Callable) -> F:
    """wrap the given into a coroutine function and try
    calling it
    """

    #@functools.wraps(func)
    async def wrapped(*args, **kwargs):
        result = func(*args, **kwargs)

        if isinstance(result, F):
            result = result.func

        is_coroutine_function = iscoroutinefunction(result)
        if is_coroutine_function:
            # call with no parameter here ?
            result = await result()
        elif iscoroutine(result):
            result = await result
        return result

    return F(wrapped)


def lift(func, *args, **kwargs) -> F:
    """make a partial function of the given func"""
    new_func = functools.partial(func, *args, **kwargs)
    return F(new_func) >> async_wrap


apartial = partial = lift


def f1(func: Callable, extractor: Callable = None) -> F:
    """ return a one argument function"""

    #@functools.wraps(func)
    def wrapped(arg1):
        value = arg1 if extractor is None else extractor(arg1)
        return func(value)

    return F(wrapped)


F1 = f1


def f0(func: Callable) -> F:
    #@functools.wraps(func)
    def wrapped(arg1):
        return func()

    return F(wrapped)


F0 = f0


def may_fail(func: Callable) -> F:
    """ simply for readability"""
    return F(func)


breaker = erratic = may_fail


def spread_args(func):
    """create a function which takes an iterable of args
    and spread it into the given function"""

    #@functools.wraps(func)
    def wrapped(args):
        return func(*args)

    return F(wrapped)


spread = spread_args


def side_effect(*func) -> List:
    """
    take an array of function and tag it as side effects
    function
    """

    def make_impure(f):
        # automatically create new function when the function
        # is a bound method and has no other input args
        f_args = getfullargspec(f).args

        if not isinstance(f, F):
            if not f_args or len(f_args) == 1 and f_args[0] == "self":
                f = F0(f)
            else:
                f = F(f)
        f.__side_effect__ = True
        return f

    return [make_impure(fu) for fu in func]


imp = impure = side_effect


def ensure_callable(x):
    if not callable(x):

        def wrapped(*args, **kwargs):
            return x

        return wrapped
    return x
