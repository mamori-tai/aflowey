from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
from enum import Enum, auto
from typing import Union, Any, Mapping, Optional

from aflowey.context import Context, ctx_var, executor_var
from aflowey.types import Opt, Executor


class ExecutorType(Enum):
    THREAD_POOL = auto()
    PROCESS_POOL = auto()


def init_executor_if_needed(
    executor: Opt[Union[Executor, ExecutorType]], **kwargs: Any
) -> Opt[Executor]:
    if executor is None:  # pragma: no cover
        return None
    if isinstance(executor, ExecutorType):
        if executor is ExecutorType.THREAD_POOL:
            return ThreadPoolExecutor(**kwargs)
        elif executor is ExecutorType.PROCESS_POOL:
            return ProcessPoolExecutor(**kwargs)
        raise ValueError("Wrong provided executor type")  # pragma: no cover
    return executor


class AsyncRunner:
    """
    Runner which holds execution options

    >>> async_exec = aexec() | flow1 | flow2 | [flow3, flow4]
    >>> with async_exec.thread_runner(max_workers=4) as runner:
    >>>     a, b, (c, d) = await runner.run(async_exec)
    """

    def __init__(
        self, func, executor: Union[Executor, Opt[ExecutorType]] = None, **kwargs
    ):
        self.func = func
        self._executor = init_executor_if_needed(executor, **kwargs)
        self._result: Optional[Any] = None
        self._ctx = None
        self._token = None

    def __enter__(self) -> "AsyncRunner":
        """if no executor provided, raise an error as the use of the with
        keyword is useless. Creates the context of the current executor"""
        # do not override it
        if self._executor is None:
            raise ValueError(
                "Trying to use with context with not executor provided"
            )  # pragma: no cover
        self._executor.__enter__()
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """closing the current executor"""
        if self._executor is None:
            raise ValueError(
                "Trying to use with context with not executor provided"
            )  # pragma: no cover
        self._executor.__exit__(exc_type, exc_val, exc_tb)

    async def run(self, context: Opt[Union[Context, Mapping]] = None, **kwargs):
        """all kwargs stuff will be passed to the executor

        Args:
            context: context var variable
            kwargs: passed to the gather function which run coroutine simultaneously
        """
        _ctx = context or {}
        _ctx = _ctx if isinstance(_ctx, Context) else Context(**_ctx)

        self._token = ctx_var.set(_ctx)
        executor_var.set(self._executor)
        # run the function of the executor
        self._result = await self.func(**kwargs)

        # reset to avoid memory leak
        ctx_var.reset(self._token)
        return self._result
