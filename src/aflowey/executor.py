import asyncio
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
from enum import Enum, auto
from typing import List, Any, Awaitable, Union, cast

from loguru import logger

from aflowey import AsyncFlow, aflow, lift
from aflowey.single_executor import SingleFlowExecutor

FlowOrListFlow = Union[List[AsyncFlow], AsyncFlow]


class ExecutorType(Enum):
    THREAD_POOL = auto()
    PROCESS_POOL = auto()


class AsyncFlowExecutor:
    """
    Execute several flows in parallel

    >>>await (executor(flows) | flow).run()

    """

    def __init__(
        self,
        /,
        executor: Union[ThreadPoolExecutor, ProcessPoolExecutor, ExecutorType] = None,
        **kwargs,
    ) -> None:
        if executor is None:
            self.executor = None
        elif isinstance(executor, ExecutorType):
            self._init_executor_if_needed(executor, **kwargs)
        else:
            self.executor = executor

        self.flows = []

    def __or__(self, flow: List[AsyncFlow]) -> "AsyncFlowExecutor":
        """add a flow to execute in parallel"""
        self.flows.append(flow)

        return self

    def __enter__(self):
        if self.executor is None:
            raise ValueError("Trying to use with context with not executor provided")
        self.executor.__enter__()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.executor.__exit__(exc_type, exc_val, exc_tb)

    @staticmethod
    async def _execute_one_flow(flow: AsyncFlow, executor) -> Any:
        return await SingleFlowExecutor(flow, executor).execute_flow()

    def exec_or_gather(self, flow: FlowOrListFlow) -> Awaitable[Any]:
        execute = self._execute_one_flow
        if isinstance(flow, list):
            return asyncio.gather(*[execute(flow, self.executor) for flow in flow])
        return execute(flow, self.executor)

    async def run(self, **kwargs: Any) -> Any:
        """main function to run stuff in parallel"""
        # if other flow run in parallel
        flows = [self.exec_or_gather(flow) for flow in self.flows]
        results = await asyncio.gather(*flows, **kwargs)
        return results

    def _init_executor_if_needed(self, executor_kind, **kwargs):
        logger.debug(executor_kind is ExecutorType.THREAD_POOL)
        if executor_kind is ExecutorType.THREAD_POOL:
            self.executor = ThreadPoolExecutor(**kwargs)
        elif executor_kind.value == ExecutorType.PROCESS_POOL:
            self.executor = ProcessPoolExecutor(**kwargs)
        else:
            raise ValueError("Wrong provided executor type")

    @staticmethod
    def ensure_flow(value: Any) -> AsyncFlow:
        if isinstance(value, AsyncFlow):
            return value
        return cast(AsyncFlow, aflow.empty() >> value)

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
    def executor(executor=None, **kwargs):
        return AsyncFlowExecutor(executor=executor, **kwargs)

    @staticmethod
    def starmap(flows=[], transformer_func=None):
        assert isinstance(flows, list), "flows must be a list of AsyncFlow or aws"

        async def wrapper(x):
            new_flows = [
                AsyncFlowExecutor.ensure_flow(lift(value, x)) for value in flows
            ]
            result = await AsyncFlowExecutor().from_flows(new_flows).run()
            return result[0]

        return wrapper


async_exec = aexec = AsyncFlowExecutor
astarmap = AsyncFlowExecutor.starmap
