import asyncio
from typing import List, Any, Awaitable, Union, cast, overload

from aflowey import AsyncFlow, aflow
from aflowey.single_executor import SingleFlowExecutor

FlowOrListFlow = Union[List[AsyncFlow], AsyncFlow]


class AsyncFlowExecutor:
    """
    Execute several flows in parallel

    >>>await (executor(flows) | flow).run()

    """

    def __init__(self, flows: Union[AsyncFlow, List[AsyncFlow]] = []) -> None:
        self.flows = []
        if flows:
            self.flows.append(flows)

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
    def ensure_flow(value: Any) -> AsyncFlow:
        if isinstance(value, AsyncFlow):
            return value
        return cast(AsyncFlow, aflow.empty() >> value)

    @staticmethod
    @overload
    def executor(flows: Union[Any, AsyncFlow]) -> "AsyncFlowExecutor":
        ...  # pragma: no cover

    @staticmethod
    @overload
    def executor(flows: List[Union[Any, AsyncFlow]]) -> "AsyncFlowExecutor":
        ...  # pragma: no cover

    @staticmethod
    def executor(flows: Any) -> "AsyncFlowExecutor":
        """create a new executor from one flow or array of flows"""
        if not isinstance(flows, list):
            flows = AsyncFlowExecutor.ensure_flow(flows)
            return AsyncFlowExecutor(flows)

        new_flows = [AsyncFlowExecutor.ensure_flow(value) for value in flows]
        executor = AsyncFlowExecutor(new_flows)
        return executor


async_exec = aexec = AsyncFlowExecutor.executor
