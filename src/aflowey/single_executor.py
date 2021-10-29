from typing import Any, Union, List, cast

from loguru import logger

from aflowey import AsyncFlow, F
from aflowey.functions import is_f0, is_side_effect


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
        if hasattr(func, "__named__"):
            return cast(str, func.__named__)  # type: ignore
        return str(index)

    def save_step(self, task: F, index: int, current_args: Any) -> None:
        """save step state in flow attribute"""
        self.flow.steps[self.get_step_name(task, index)] = current_args

    def _check_current_args_if_side_effect(self, first_aws: F, res: Any) -> Any:
        if is_side_effect(first_aws):
            return self.flow.kwargs if self.flow.kwargs else self.flow.args
        return res

    async def execute_first_step(self, first_aws: F) -> Any:
        """executing the first step"""
        if not self.flow.args and not self.flow.kwargs and is_f0(first_aws):
            self.flow.args = (None,)

        res = await first_aws(*self.flow.args, **self.flow.kwargs)
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
        current_args = await self.execute_first_step(first_aws)

        # iterate over its tasks
        for index, task in enumerate(self.flow.aws[1:]):
            # get the result of a task
            result = await task(current_args)

            if is_side_effect(task):
                # side effect task, does not return a value
                self.save_step(task, index + 1, current_args)
                if self.need_to_cancel_flow(result):
                    break  # pragma: no cover
                continue  # pragma: no cover

            result = await self.check_and_execute_flow_if_needed(result)
            # cancel ?
            if self.need_to_cancel_flow(result):
                break

            current_args = result
            self.save_step(task, index + 1, current_args)
        # return current args that are the actual results
        return current_args


CANCEL_FLOW = SingleFlowExecutor.CANCEL_FLOW
FlowOrListFlow = Union[List[AsyncFlow], AsyncFlow]
