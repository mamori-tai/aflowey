import functools
from inspect import getfullargspec
from inspect import iscoroutine
from typing import Any
from typing import Callable
from typing import List


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

        @functools.wraps(other)
        def wrapped(*args, **kwargs):
            return other(self.func(*args, **kwargs))

        return F(wrapped)

    def __call__(self, *args, **kwargs) -> Any:
        """ simply call the inner function"""
        return self.func(*args, **kwargs)

    def __repr__(self) -> str:
        return f"<F instance: {repr(self.func)}>"


def async_wrap(func: Callable) -> F:
    """wrap the given into a coroutine function and try
    calling it
    """

    @functools.wraps(func)
    async def wrapped(*args, **kwargs):
        async def _exec(function, *a, **kw):
            current_result = function(*a, **kw)
            if iscoroutine(current_result):
                current_result = await current_result
                if isinstance(current_result, F):
                    return await _exec(current_result.func)
            return current_result

        return await _exec(func, *args, **kwargs)

    return F(wrapped)


def lift(func, *args, **kwargs) -> F:
    """make a partial function of the given func and ensure
    it will work in an async context
    """
    new_func = functools.partial(func, *args, **kwargs)
    return F(new_func) >> async_wrap


apartial = partial = lift


def f1(func: Callable, extractor: Callable = None) -> F:
    """wraps a one argument function (with arity 1) and allows
        to add an extractor to work on the input argument.
    """

    @functools.wraps(func)
    def wrapped(arg1):
        value = arg1 if extractor is None else extractor(arg1)
        return func(value)
    wrapped.__F1__ = True
    return F(wrapped)


F1 = f1


def f0(func: Callable) -> F:
    """create a new function from a 0 arity function (takes 0 args). The new
     function takes exactly one argument and does not pass it to the wrapped
     function. It allows using a 0 arity function in a flow relatively easily.
    """
    @functools.wraps(func)
    def wrapped(arg1):
        return func()
    wrapped.__F0__ = True
    return F(wrapped)


F0 = f0


def may_fail(func: Callable) -> F:
    """ simply for readability"""
    return func


breaker = erratic = may_fail


def spread_args(func):
    """create a function which takes an iterable of args
    and spread it into the given function"""

    @functools.wraps(func)
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

    if len(func) == 1:
        return make_impure(func[0])
    return [make_impure(fu) for fu in func]


imp = impure = side_effect


def ensure_callable(x: Any) -> Callable:
    if not callable(x):

        def wrapped(*args, **kwargs):
            return x

        return wrapped
    return x
