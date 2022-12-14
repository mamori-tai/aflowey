import asyncio
import functools
from concurrent.futures import ProcessPoolExecutor
from contextvars import copy_context
from inspect import isawaitable
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
from aflowey.types import Function


async def _exec(function: Union[F, Function], *a: Any, **kw: Any) -> Any:
    current_result = function(*a, **kw)
    while asyncio.iscoroutine(current_result) or isawaitable(current_result):
        current_result = await current_result
    if isinstance(current_result, F):
        return await _exec(current_result)
    return current_result


def check_and_run_step(fn: F, *args: Any, **kwargs: Any) -> Awaitable[Any]:
    executor = executor_var.get()

    if fn.is_coroutine_function:
        return _exec(fn, *args, **kwargs)

    new_fn = functools.partial(fn.func, *args, **kwargs)

    loop = asyncio.get_event_loop()

    # process executor does not have access to the context data
    if isinstance(executor, ProcessPoolExecutor):
        logger.debug(f'running "{new_fn}" in a process pool executor')
        return loop.run_in_executor(executor, new_fn)

    context = copy_context()

    logger.debug(f'running "{new_fn}" in a thread pool executor')
    return loop.run_in_executor(executor, context.run, new_fn)


class SingleFlowExecutor:
    """Single flow executor"""

    CANCEL_FLOW = object()

    def __init__(self, flow: AsyncFlow) -> None:
        self.flow = flow

    @staticmethod
    async def check_and_execute_flow_if_needed(
        maybe_flow: Union[Any, AsyncFlow], **kwargs: Any
    ) -> Any:
        """check if we have an async flow and execute it"""
        if isinstance(maybe_flow, AsyncFlow):
            return await SingleFlowExecutor(maybe_flow).execute_flow(
                is_root=False, **kwargs
            )
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

    def _check_current_args_if_side_effect(self, first_aws: F, res: Any) -> Any:
        if is_side_effect(first_aws):
            return self._get_result_from_early_abort()
            # self.flow.kwargs if self.flow.kwargs else self.flow.args
        return res

    async def _execute_first_step(self, first_aws: F, **kwargs: Any) -> Any:
        """executing the first step"""
        if not self.flow.args and not self.flow.kwargs and is_f0(first_aws):
            self.flow.args = (None,)

        res = await check_and_run_step(first_aws, *self.flow.args, **self.flow.kwargs)
        current_args = self._check_current_args_if_side_effect(first_aws, res)
        # if flow run it
        current_args = await self.check_and_execute_flow_if_needed(
            current_args, **kwargs
        )

        # memorize name
        self.save_step(first_aws, 0, current_args)
        return current_args

    def _get_result_from_early_abort(self):
        if self.flow.kwargs:
            return self.flow.kwargs
        if not self.flow.args:
            return None
        if self.flow.args and len(self.flow.args) == 1:
            return self.flow.args[0]
        return self.flow.args

    async def execute_flow(self, is_root: bool, **kwargs: Any) -> Any:
        """Main function to execute a flow"""
        if not self.flow.aws:
            return None

        # get first step
        try:
            first_aws = self.flow.aws[0]
            current_args = await self._execute_first_step(first_aws, **kwargs)

            if self.need_to_cancel_flow(current_args):
                # returning canceling flow
                if is_root:
                    return self._get_result_from_early_abort()
                return current_args

            for index, task in enumerate(self.flow.aws[1:]):

                result = await check_and_run_step(task, current_args)

                if is_side_effect(task):
                    # side effect task, does not return a value
                    self.save_step(task, index + 1, current_args)
                    if self.need_to_cancel_flow(result):
                        break  # pragma: no cover
                    continue  # pragma: no cover

                result = await self.check_and_execute_flow_if_needed(result, **kwargs)
                if self.need_to_cancel_flow(
                    result
                ):  # check if we need to cancel the flow
                    break

                current_args = result
                self.save_step(task, index + 1, current_args)
        except Exception as e:
            logger.error(e)
            return_exceptions: bool = kwargs.pop("return_exceptions", False)
            # setting flow information
            self.flow.is_success = False
            if return_exceptions is False:
                raise e
            return e
        else:
            self.flow.is_success = True
            return current_args
        finally:
            self.flow.executed = True


CANCEL_FLOW = SingleFlowExecutor.CANCEL_FLOW
