from concurrent.futures import ProcessPoolExecutor
from concurrent.futures import ThreadPoolExecutor
from typing import Any
from typing import Callable
from typing import Coroutine
from typing import Optional
from typing import TypeVar
from typing import Union


Opt = Optional
Function = TypeVar("Function", bound=Callable[..., Any])
Executor = Optional[Union[ThreadPoolExecutor, ProcessPoolExecutor]]
AnyCoroutineFunction = Callable[[Any, Any], Coroutine[Any, Any, Any]]
AnyCallable = Callable[[Any], Any]
