import asyncio
from typing import Any
from typing import Awaitable
from typing import List
from typing import Union
from typing import cast

from aflowey import AsyncFlow
from aflowey import aflow
from aflowey.runner import AsyncRunner, ExecutorType
from aflowey.single_executor import SingleFlowExecutor
from aflowey.types import AnyCallable

FlowOrListFlow = Union[List[AsyncFlow], AsyncFlow]


class AsyncFlowExecutor:
    """
    Execute several flows concurrently

    >>> await (aexec().from_flows(flows) | flow).run()

    """

    def __init__(self) -> None:
        """
        Creates a new async flow executor
        """
        self.flows: List[Union[AsyncFlow, List[AsyncFlow]]] = []

    def __or__(self, flow: List[AsyncFlow]) -> "AsyncFlowExecutor":
        """add a flow to execute in parallel"""
        return self.from_flows(flow)

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

    def thread_runner(self, **kwargs):
        return AsyncRunner(self.run, ExecutorType.THREAD_POOL, **kwargs)

    def process_runner(self, **kwargs):
        return AsyncRunner(self.run, ExecutorType.PROCESS_POOL, **kwargs)

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
        async def wrapper(arg: Any) -> Any:
            new_flows = [AsyncFlowExecutor.ensure_flow(fn, arg) for fn in flows]
            result = (
                await AsyncFlowExecutor().from_flows(new_flows).run()
            )
            return result[0]

        return wrapper


async_exec = aexec = AsyncFlowExecutor
flows_from_arg = spawn_flows = run_flows = astarmap = AsyncFlowExecutor.starmap
