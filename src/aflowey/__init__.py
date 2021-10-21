import asyncio
from copy import copy
from inspect import iscoroutinefunction
from typing import Any
from typing import Coroutine
from typing import List
from typing import overload
from typing import Union

from loguru import logger

from aflowey.f import async_wrap
from aflowey.f import ensure_callable
from aflowey.f import F
from aflowey.f import side_effect


class AsyncFlow:
    """

    flow = (Flow() >> gen1 >> gen2 >> gen3) | (Flow()
    await flow.run()
    """

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
            func = side_effect(func)

        return func

    def __rshift__(self, aws) -> "AsyncFlow":
        if isinstance(aws, list):
            self.aws += [self.ensure_coroutine_func(a) for a in aws]
        else:
            self.aws.append(self.ensure_coroutine_func(aws))

        return self

    def __copy__(self):
        """make a shallow copy"""
        aws = self.aws[:]
        args = self.args[:]
        kwargs = self.kwargs.copy()
        new_flow = self.__class__(*args, **kwargs)
        new_flow.aws = aws
        return new_flow

    async def run(self, **kwargs):
        """kwargs passed directly to asyncio.gather function"""
        return await _FlowExecutor(self).execute_flow(**kwargs)

    @staticmethod
    def log(log_str, print_arg=False):
        async def wrapped(last_result):
            logger.info(log_str)
            if print_arg:  # pragma: no cover
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
        """copy the flow and return it"""
        return copy(flow)


# aliases
aflow = async_flow = AsyncFlow
flog = aflow.log


class _FlowExecutor:
    """Single flow executor"""

    CANCEL_FLOW = object()

    def __init__(self, flow):
        self.flow = flow

    @staticmethod
    async def check_and_execute_flow_if_needed(maybe_flow):
        """check if we have an async flow and execute it"""
        if isinstance(maybe_flow, AsyncFlow):
            return await _FlowExecutor(maybe_flow).execute_flow()
        return maybe_flow

    @staticmethod
    def need_to_cancel_flow(result: Any):
        if result is _FlowExecutor.CANCEL_FLOW:
            logger.info("Received sentinel object, canceling flow...")
            return True
        return False

    async def execute_flow(self, **kwargs):
        """Main function to execute a flow"""
        if not self.flow.aws:
            logger.debug("no aws")
            return None

        # get first step
        current_args = await self.flow.aws[0](*self.flow.args, *self.flow.kwargs)

        # maybe the step is an async flow
        current_args = await self.check_and_execute_flow_if_needed(current_args)

        # iterate over its tasks
        for task in self.flow.aws[1:]:
            # side effect task, does not return a value
            # reuse the current args
            if hasattr(task, "__side_effect__"):
                result = await task(current_args)
                if self.need_to_cancel_flow(result):
                    break  # pragma: no cover
                continue  # pragma: no cover

            # get the result of a task
            result = await task(current_args)
            result = await self.check_and_execute_flow_if_needed(result)
            # cancel ?
            if self.need_to_cancel_flow(result):
                break
            current_args = result
        # return current args that are the actual results
        return current_args


CANCEL_FLOW = _FlowExecutor.CANCEL_FLOW
FlowOrListFlow = Union[List[AsyncFlow], AsyncFlow]


class AsyncFlowExecutor:
    """

    (executor(flows) | flow).run()

    """

    def __init__(self, flows: List[AsyncFlow] = [], run_in_thread_pool=False):
        self.flows = []
        self.flows.append(flows)
        # to be run in parallel
        self.run_in_thread_pool = run_in_thread_pool

    def __or__(self, flow: FlowOrListFlow) -> "AsyncFlowExecutor":
        self.flows.append(flow)

        return self

    @staticmethod
    async def _execute_one_flow(flow: "Flow", *args, **kwargs) -> Any:
        return await _FlowExecutor(flow, *args, **kwargs).execute_flow()

    def exec_or_gather(self, flow: FlowOrListFlow) -> Coroutine:
        execute = self._execute_one_flow
        if isinstance(flow, list):
            return asyncio.gather(
                *[execute(flow, *flow.args, **flow.kwargs) for flow in flow]
            )
        return execute(flow, *flow.args, **flow.kwargs)

    async def run(self, **kwargs):
        """main function to run stuff in parallel"""
        # if other flow run in parallel
        flows = [self.exec_or_gather(flow) for flow in self.flows]
        results = await asyncio.gather(*flows, **kwargs)
        return results

    @staticmethod
    def executor(flows):
        return AsyncFlowExecutor(flows)


async_exec = aexec = AsyncFlowExecutor.executor
