import functools
from typing import Any
from typing import Callable

from aflowey.types import Function


class F:
    """tiny wrapper around a function

    Args:
        func: callable

    """

    def __init__(self, func: Function) -> None:
        self.func = func
        functools.update_wrapper(self, func)

    def __rshift__(self, other: Callable[[Function], 'F']) -> 'F':
        """equivalent of decoration"""
        return F(other(self.func))

    def __gt__(self, other: Function) -> 'F':
        """function composition"""

        @functools.wraps(other)
        def wrapped(*args: Any, **kwargs: Any) -> Any:
            return other(self.func(*args, **kwargs))

        return F(wrapped)

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        """ simply call the inner function"""
        return self.func(*args, **kwargs)

    def __repr__(self) -> str:
        return f"<F instance: {repr(self.func)}>"

