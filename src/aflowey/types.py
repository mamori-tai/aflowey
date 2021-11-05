from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
from typing import Any, TypeVar, Callable, Optional, Union, Coroutine

from mypy_extensions import VarArg, KwArg

Opt = Optional
Function = TypeVar("Function", bound=Callable[..., Any])
Executor = Optional[Union[ThreadPoolExecutor, ProcessPoolExecutor]]
AnyCoroutineFunction = Callable[[VarArg(Any), KwArg(Any)], Coroutine[Any, Any, Any]]
AnyCallable = Callable[[Any], Any]
