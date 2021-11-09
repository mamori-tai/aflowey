import asyncio
import functools
from typing import Any

from aflowey.types import Function


class F:
    """tiny wrapper around a function

    Args:
        func: callable

    """

    def __init__(self, func: Function) -> None:
        self.func = func
        if self.func is not None:
            functools.update_wrapper(self, func)

    def __rshift__(self, other: Any) -> "F":
        """equivalent of decoration"""
        if self.func is None:
            return F(other)

        return F(other(self.func))

    def __gt__(self, other: Function) -> "F":
        """function composition"""

        @functools.wraps(other)
        def wrapped(*args: Any, **kwargs: Any) -> Any:
            return other(self.func(*args, **kwargs))

        return F(wrapped)

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        """simply call the inner function"""
        return self.func(*args, **kwargs)

    def __repr__(self) -> str:
        return f"<F instance: {repr(self.func)}>"

    @property
    def is_coroutine_function(self) -> bool:
        func = self.func
        while isinstance(func, F):
            func = func.func
        return asyncio.iscoroutinefunction(func)


FF = F(None)  # type: ignore
