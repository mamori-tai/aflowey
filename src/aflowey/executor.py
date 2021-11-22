import asyncio
from concurrent.futures import ProcessPoolExecutor
from concurrent.futures import ThreadPoolExecutor
from enum import Enum
from enum import auto
from typing import Any
from typing import Awaitable
from typing import List
from typing import Optional
from typing import Union
from typing import cast

from aflowey import AsyncFlow
from aflowey import aflow
from aflowey.context import executor_var, ctx_var, Context
from aflowey.single_executor import SingleFlowExecutor
from aflowey.types import AnyCallable
from aflowey.types import Executor

FlowOrListFlow = Union[List[AsyncFlow], AsyncFlow]


class ExecutorType(Enum):
    THREAD_POOL = auto()
    PROCESS_POOL = auto()


class AsyncFlowExecutor:
    """
    Execute several flows concurrently

    >>>await (aexec().from_flows(flows) | flow).run()

    """

    def __init__(
        self, executor: Union[Executor, Optional[ExecutorType]] = None, **kwargs: Any
    ) -> None:
        """
        Creates a new async flow executor
        Args:
            executor: the executor to process synchronous code, could be a ThreadPoolExecutor
            ProcessPoolExecutor
            kwargs: all passed to create the executor
        """
        if "context" in kwargs:
            self._context = Context(**kwargs["context"])
            ctx_var.set(self._context)

        # do not override it
        if executor_var.get() is None:
            executor_var.set(
                AsyncFlowExecutor._init_executor_if_needed(executor, **kwargs)
            )

        self.flows: List[Union[AsyncFlow, List[AsyncFlow]]] = []

    @property
    def executor(self):
        return executor_var.get()

    def __or__(self, flow: List[AsyncFlow]) -> "AsyncFlowExecutor":
        """add a flow to execute in parallel"""
        self.flows.append(flow)

        return self

    def __enter__(self) -> "AsyncFlowExecutor":
        """if no executor provided, raise an error as the use of the with
        keyword is useless. Creates the context of the current executor"""
        if self.executor is None:
            raise ValueError(
                "Trying to use with context with not executor provided"
            )  # pragma: no cover
        self.executor.__enter__()
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """closing the current executor"""

        if self.executor is None:
            raise ValueError(
                "Trying to use with context with not executor provided"
            )  # pragma: no cover
        self.executor.__exit__(exc_type, exc_val, exc_tb)
        ctx_var.set(None)
        executor_var.set(None)

    @staticmethod
    async def _execute_one_flow(flow: AsyncFlow) -> Any:
        """Run"""
        return await SingleFlowExecutor(flow).execute_flow()

    def _execute_or_gather(self, flow: FlowOrListFlow) -> Awaitable[Any]:
        if isinstance(flow, list):
            flows_task = [self._execute_one_flow(flow) for flow in flow]
            return asyncio.gather(*flows_task)
        return self._execute_one_flow(flow)

    def run(self, **kwargs: Any) -> Any:
        """main function to run stuff in parallel"""
        # if other flow run in parallel
        flows = [self._execute_or_gather(flow) for flow in self.flows]
        return asyncio.gather(*flows, **kwargs)

    @staticmethod
    def _init_executor_if_needed(
        executor: Optional[Union[Executor, ExecutorType]], **kwargs: Any
    ) -> Optional[Executor]:
        if executor is None:
            return None
        if isinstance(executor, ExecutorType):
            if executor is ExecutorType.THREAD_POOL:
                kw = {**kwargs}
                if "context" in kw:
                    del kw["context"]
                return ThreadPoolExecutor(**kw)
            elif executor is ExecutorType.PROCESS_POOL:
                return ProcessPoolExecutor(**kwargs)
            raise ValueError("Wrong provided executor type")
        return executor

    @staticmethod
    def ensure_flow(fn: Any, arg: Any = None) -> AsyncFlow:
        if isinstance(fn, AsyncFlow):
            return fn
        flow = aflow.empty() if arg is None else aflow.from_args(arg)
        return cast(AsyncFlow, flow >> fn)

    def from_flows(self, flows: Any) -> "AsyncFlowExecutor":
        """create a new executor from one flow or array of flows"""
        if not isinstance(flows, list):
            flows = AsyncFlowExecutor.ensure_flow(flows)
            self.flows.append(flows)
        else:
            new_flows = [AsyncFlowExecutor.ensure_flow(value) for value in flows]
            self.flows.append(new_flows)
        return self

    @staticmethod
    def starmap(*flows: Any) -> AnyCallable:
        executor = executor_var.get()

        async def wrapper(arg: Any) -> Any:
            new_flows = [AsyncFlowExecutor.ensure_flow(fn, arg) for fn in flows]
            result = (
                await AsyncFlowExecutor(executor=executor).from_flows(new_flows).run()
            )
            return result[0]

        return wrapper


async_exec = aexec = AsyncFlowExecutor
flows_from_arg = spawn_flows = run_flows = astarmap = AsyncFlowExecutor.starmap
