from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
from typing import Any, TypeVar, Callable, Optional, Union, Coroutine


Opt = Optional
Function = TypeVar("Function", bound=Callable[..., Any])
Executor = Optional[Union[ThreadPoolExecutor, ProcessPoolExecutor]]
AnyCoroutineFunction = Callable[[Any, Any], Coroutine[Any, Any, Any]]
AnyCallable = Callable[[Any], Any]
