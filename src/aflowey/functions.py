import functools
import inspect
from typing import Any, Callable, List, Union, Iterable, Optional, cast

from aflowey import F
from aflowey.types import Function


def async_wrap(func: F) -> F:
    """wrap the given into a coroutine function and try
    calling it
    """

    @functools.wraps(func)
    async def wrapped(*args: Any, **kwargs: Any) -> Any:
        async def _exec(function: Union[F, Function], *a: Any, **kw: Any) -> Any:
            current_result = function(*a, **kw)
            if inspect.iscoroutine(current_result):
                current_result = await current_result
                if isinstance(current_result, F):
                    return await _exec(current_result.func)
            return current_result

        return await _exec(func, *args, **kwargs)

    return F(wrapped)


def lift(func: Function, *args: Any, **kwargs: Any) -> F:
    """make a partial function of the given func and ensure
    it will work in an async context
    """
    new_func = cast(Callable[[], Any], functools.partial(func, *args, **kwargs))
    return F(new_func) >> async_wrap


apartial = partial = lift


def f1(func: Function, extractor: Optional[Function] = None) -> F:
    """wraps a one argument function (with arity 1) and allows
    to add an extractor to work on the input argument.
    """

    @functools.wraps(func)
    def wrapped(arg1: Any) -> Any:
        value = arg1 if extractor is None else extractor(arg1)
        return func(value)

    wrapped.__F1__ = True  # type: ignore
    return F(wrapped)


F1 = f1


def f0(func: Function) -> F:
    """create a new function from a 0 arity function (takes 0 args). The new
    function takes exactly one argument and does not pass it to the wrapped
    function. It allows using a 0 arity function in a flow relatively easily.
    """

    @functools.wraps(func)
    def wrapped(arg1: Any) -> Any:
        return func()

    wrapped.__F0__ = True  # type: ignore
    return F(wrapped)


F0 = f0


def may_fail(func: Function) -> Function:
    """ simply for readability"""
    return func


breaker = erratic = may_fail


def spread_args(func: Function) -> F:
    """create a function which takes an iterable of args
    and spread it into the given function"""

    @functools.wraps(func)
    def wrapped(args: Iterable[Any]) -> Any:
        # if to much args, slice the args to given length
        return func(*args)

    return F(wrapped)


spread = spread_args


def spread_kwargs(func: Function) -> F:
    """create a function which takes a mapping of kwargs
    and spread it into the given function"""

    @functools.wraps(func)
    def wrapped(**kwargs: Any) -> Any:
        arg_spec = inspect.getfullargspec(func)
        args = set(arg_spec.args + arg_spec.kwonlyargs) - set(["self"])
        new_kwargs = {key: kwargs[key] for key in args}
        return func(**new_kwargs)

    return F(wrapped)


spread_kw = spread_kwargs


def ensure_f(func: Function) -> F:
    if not isinstance(func, F):
        return F(func)
    return func


def make_impure(func: Union[Function, F]) -> F:
    # automatically create new function when the function
    # is a bound method and has no other input args
    func = ensure_f(func)
    func.__side_effect__ = True  # type: ignore
    return func


def side_effect(*func: Function) -> Union[List[F], F]:
    """
    take an array of function and tag it as side effects
    function
    """
    if len(func) == 1:
        return make_impure(func[0])
    return [make_impure(fu) for fu in func]


imp = impure = side_effect


def ensure_callable(x: Union[Any, Function]) -> Function:
    """ ensure a given args is a callable by returning a new callable if not"""
    if not callable(x):

        def wrapped(*args: Any, **kwargs: Any) -> Any:
            return x

        return cast(Function, wrapped)
    return cast(Function, x)


def named(func: Union[Function, F], name: str) -> F:
    if not callable(func):
        func = ensure_callable(func)
    func = ensure_f(func)
    func.__named__ = name  # type: ignore
    return func


def identity(x: Any) -> Any:
    return x


# helper functions
def is_f0(func: Function) -> bool:
    return hasattr(func, "__F0__")


def is_f1(func: Function) -> bool:
    return hasattr(func, "__F1__")


def is_side_effect(func: Function) -> bool:
    return hasattr(func, "__side_effect__")


def get_name(func: Function) -> bool:
    if hasattr(func, "__named__"):
        return func.__named__
    return ""
