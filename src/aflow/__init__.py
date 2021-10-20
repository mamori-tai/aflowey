import asyncio
from inspect import iscoroutinefunction
from typing import Any, Union, List, Coroutine, overload

from loguru import logger

from aflow.f import F, async_wrap, side_effect, ensure_callable


class AsyncFlow:
    """

    flow = (Flow() >> gen1 >> gen2 >> gen3) | (Flow()
    await flow.run()
    """

    CANCEL_FLOW = object()

    def __init__(self, *args, **kwargs):
        # first arguments input
        self.args = args
        self.kwargs = kwargs

        # function to be executed in the flow
        self.aws = []

    @staticmethod
    def ensure_coroutine_func(func):
        side_effect_func = hasattr(func, "__side_effect__")
        func = ensure_callable(func)
        if not iscoroutinefunction(func):
            func = F(func) >> async_wrap

        if side_effect_func:
            func = side_effect(func)[0]

        return func

    def __rshift__(self, aws) -> "AsyncFlow":
        if isinstance(aws, list):
            self.aws += [self.ensure_coroutine_func(a) for a in aws]
        else:
            self.aws.append(self.ensure_coroutine_func(aws))

        return self

    async def run(self, **kwargs):
        """kwargs passed directly to asyncio.gather function"""
        return await _FlowExecutor(self).execute_flow(**kwargs)

    @staticmethod
    def log(log_str, print_arg=False):
        async def wrapped(last_result):
            logger.info(log_str)
            if print_arg:
                logger.info(last_result)
            return last_result

        return wrapped

    @staticmethod
    def from_args(*args, **kwargs) -> "AsyncFlow":
        return AsyncFlow(*args, **kwargs)

    @staticmethod
    def empty() -> "AsyncFlow":
        return AsyncFlow()

    @staticmethod
    def from_flow(flow) -> "AsyncFlow":
        """for readability"""
        return flow


# aliases
aflow = async_flow = AsyncFlow
flog = aflow.log


class _FlowExecutor:
    """Single flow executor"""

    def __init__(self, flow):
        self.flow = flow

    @staticmethod
    async def check_and_execute_flow_if_needed(maybe_flow):
        """check if we have an async flow and execute it"""
        if isinstance(maybe_flow, AsyncFlow):
            return await _FlowExecutor(maybe_flow).execute_flow()
        return maybe_flow

    async def execute_flow(self, **kwargs):
        """Main function to execute a flow"""
        if not self.flow.aws:
            logger.debug("no aws")
            return None
        current_args = await self.flow.aws[0](*self.flow.args, *self.flow.kwargs)
        current_args = await self.check_and_execute_flow_if_needed(current_args)

        for task in self.flow.aws[1:]:
            if hasattr(task, "__side_effect__"):
                result = await task(current_args)
                if result is AsyncFlow.CANCEL_FLOW:
                    logger.info("Received sentinel object, canceling flow...")
                    break
                continue

            current_args = await task(current_args)

            current_args = await self.check_and_execute_flow_if_needed(current_args)

        # return current args that are the actual results
        return current_args


FlowOrListFlow = Union[List[AsyncFlow], AsyncFlow]


class AsyncFlowExecutor:
    """

    (executor(flows) | flow).run()

    """

    def __init__(self, flows: List[AsyncFlow] = [], run_in_thread_pool=False):
        self.flows = flows

        # to be run in parallel
        self.other_flows = []
        self.run_in_thread_pool = run_in_thread_pool

    def __or__(self, flow: FlowOrListFlow) -> "AsyncFlowExecutor":
        self.other_flows.append(flow)

        return self

    @staticmethod
    async def _execute_one_flow(flow: "Flow", *args, **kwargs) -> Any:
        return await _FlowExecutor(flow, *args, **kwargs).execute_flow()

    @overload
    def exec_or_gather(self, flow: List[AsyncFlow]) -> Coroutine:
        ...

    @overload
    def exec_or_gather(self, flow: AsyncFlow) -> Coroutine:
        ...

    def exec_or_gather(self, flow):
        execute = self._execute_one_flow
        if isinstance(flow, list):
            return asyncio.gather(
                *[execute(flow, *flow.args, **flow.kwargs) for flow in flow]
            )
        return execute(flow, *flow.args, **flow.kwargs)

    async def run(self, **kwargs):
        """main function to run stuff in parallel"""
        # if other flow run in parallel
        flows = [
            *[self.exec_or_gather(flow) for flow in self.flows],
            *[self.exec_or_gather(flow) for flow in self.other_flows],
        ]

        results = await asyncio.gather(*flows, **kwargs)
        return results
