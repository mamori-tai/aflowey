import asyncio
from typing import List, Any, Coroutine, Awaitable

from aflowey import AsyncFlow
from aflowey.single_executor import FlowOrListFlow, SingleFlowExecutor


class AsyncFlowExecutor:
    """
    Execute several flows in parallel

    >>>await (executor(flows) | flow).run()

    """

    def __init__(
        self, flows: List[AsyncFlow] = [], run_in_thread_pool: bool = False
    ) -> None:
        self.flows = []
        self.flows.append(flows)
        # to be run in parallel
        self.run_in_thread_pool = run_in_thread_pool

    def __or__(self, flow: List[AsyncFlow]) -> "AsyncFlowExecutor":
        """add a flow to execute in parallel"""
        self.flows.append(flow)

        return self

    @staticmethod
    async def _execute_one_flow(flow: AsyncFlow) -> Any:
        return await SingleFlowExecutor(flow).execute_flow()

    def exec_or_gather(self, flow: FlowOrListFlow) -> Awaitable[Any]:
        execute = self._execute_one_flow
        if isinstance(flow, list):
            return asyncio.gather(*[execute(flow) for flow in flow])
        return execute(flow)

    async def run(self, **kwargs: Any) -> Any:
        """main function to run stuff in parallel"""
        # if other flow run in parallel
        flows = [self.exec_or_gather(flow) for flow in self.flows]
        results = await asyncio.gather(*flows, **kwargs)
        return results

    @staticmethod
    def executor(flows: List[AsyncFlow]) -> "AsyncFlowExecutor":
        """create a new executor from one flow or array of flows"""
        return AsyncFlowExecutor(flows)


async_exec = aexec = AsyncFlowExecutor.executor
