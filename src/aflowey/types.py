from typing import Any, TypeVar, Callable

Function = TypeVar("Function", bound=Callable[..., Any])
