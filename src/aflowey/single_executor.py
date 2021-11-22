import asyncio
import functools
from contextvars import copy_context
from typing import Any
from typing import Awaitable
from typing import Union

from loguru import logger

from aflowey.async_flow import AsyncFlow
from aflowey.context import executor_var
from aflowey.f import F
from aflowey.functions import get_name
from aflowey.functions import is_f0
from aflowey.functions import is_side_effect
from aflowey.types import AnyCoroutineFunction
from aflowey.types import Function


async def _exec(function: Union[F, Function], *a: Any, **kw: Any) -> Any:
    current_result = function(*a, **kw)
    while asyncio.iscoroutine(current_result):
        current_result = await current_result
    if callable(current_result):
        return await _exec(current_result)
    return current_result


def _exec_synchronously(fn: Function, *args: Any) -> Any:
    def _run():
        result = fn(*args)
        while callable(result):
            result = result()
        return result

    return _run


def async_wrap(func: F) -> AnyCoroutineFunction:
    """wrap the given into a coroutine function and try
    calling it
    """

    @functools.wraps(func)
    async def wrapped(*args: Any, **kwargs: Any) -> Any:
        return await _exec(func, *args, **kwargs)

    return wrapped


def check_and_run_step(fn: F, *args: Any, **kwargs: Any) -> Awaitable[Any]:
    loop = asyncio.get_event_loop()

    if fn.is_coroutine_function:
        new_fn = async_wrap(fn)
        return loop.create_task(new_fn(*args, **kwargs))  # type: ignore[call-arg]

    executor = executor_var.get()

    if executor is None:
        new_fn = async_wrap(fn)
        return new_fn(*args, **kwargs)  # type: ignore[call-arg]

    new_fn = fn.func
    if kwargs:
        new_fn = functools.partial(new_fn, **kwargs)
    context = copy_context()

    logger.debug(f'running "{new_fn}" in a thread pool executor')
    return loop.run_in_executor(
        executor, context.run, _exec_synchronously(new_fn, *args)
    )


class SingleFlowExecutor:
    """Single flow executor"""

    CANCEL_FLOW = object()

    def __init__(self, flow: AsyncFlow) -> None:
        self.flow = flow

    @staticmethod
    async def check_and_execute_flow_if_needed(
        maybe_flow: Union[Any, AsyncFlow]
    ) -> Any:
        """check if we have an async flow and execute it"""
        if isinstance(maybe_flow, AsyncFlow):
            return await SingleFlowExecutor(maybe_flow).execute_flow()
        return maybe_flow

    @staticmethod
    def need_to_cancel_flow(result: Any) -> bool:
        """check if we need to cancel flow checking sentinel"""
        if result is SingleFlowExecutor.CANCEL_FLOW:
            logger.debug("Received sentinel object, canceling flow...")
            return True
        return False

    @staticmethod
    def get_step_name(func: F, index: int) -> str:
        """get the step name"""
        return get_name(func) or str(index)

    def save_step(self, task: F, index: int, current_args: Any) -> None:
        """save step state in flow attribute"""
        step_name = self.get_step_name(task, index)
        self.flow.steps[step_name] = current_args

        # update context
        # ctx = {**ctx_var.get(), step_name: current_args}
        # ctx_var.set(ctx)

    def _check_current_args_if_side_effect(self, first_aws: F, res: Any) -> Any:
        if is_side_effect(first_aws):
            return self.flow.kwargs if self.flow.kwargs else self.flow.args
        return res

    async def _execute_first_step(self, first_aws: F) -> Any:
        """executing the first step"""
        if not self.flow.args and not self.flow.kwargs and is_f0(first_aws):
            self.flow.args = (None,)

        res = await check_and_run_step(first_aws, *self.flow.args, **self.flow.kwargs)
        current_args = self._check_current_args_if_side_effect(first_aws, res)
        # if flow run it
        current_args = await self.check_and_execute_flow_if_needed(current_args)

        # memorize name
        self.save_step(first_aws, 0, current_args)
        return current_args

    async def execute_flow(self) -> Any:
        """Main function to execute a flow"""
        if not self.flow.aws:
            return None

        # get first step
        first_aws = self.flow.aws[0]
        current_args = await self._execute_first_step(first_aws)

        for index, task in enumerate(self.flow.aws[1:]):

            result = await check_and_run_step(task, current_args)

            if is_side_effect(task):
                # side effect task, does not return a value
                self.save_step(task, index + 1, current_args)
                if self.need_to_cancel_flow(result):
                    break  # pragma: no cover
                continue  # pragma: no cover

            result = await self.check_and_execute_flow_if_needed(result)
            if self.need_to_cancel_flow(result):  # check if we need to cancel the flow
                break

            current_args = result
            self.save_step(task, index + 1, current_args)
        # return current args that are the actual results
        return current_args


CANCEL_FLOW = SingleFlowExecutor.CANCEL_FLOW
