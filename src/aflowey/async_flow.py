from copy import copy
from inspect import iscoroutinefunction
from typing import Any, Union, List, Dict

from loguru import logger

from aflowey import ensure_callable, F, async_wrap
from aflowey.functions import is_side_effect, get_name
from aflowey.types import Function


class AsyncFlow:
    """
    Describe an async flow chaining function

    >>>flow = (AsyncFlow() >> gen1 >> gen2 >> gen3)
    >>>await flow.run()
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        # first arguments input
        self.args = args
        self.kwargs = kwargs

        # function to be executed in the flow
        self.aws: List[F] = []

        # intermediary store results
        self.steps: Dict[str, Any] = {}

    @staticmethod
    def ensure_coroutine_func(func: Union[Function, F]) -> F:
        side_effect_func = is_side_effect(func)
        named_func = get_name(func)

        func = ensure_callable(func)

        # ensuring F function are coroutine
        if not iscoroutinefunction(func):
            func = F(func) >> async_wrap

        # ensuring we have only F function
        if not isinstance(func, F):
            func = F(func)

        # resetting dunder custom attribute
        if side_effect_func:
            func.__side_effect__ = True  # type: ignore
        if named_func:
            func.__named__ = named_func  # type: ignore

        return func

    def __rshift__(
        self, aws: Union[List[Union[F, Function]], Union[F, Function]]
    ) -> "AsyncFlow":
        """add a new step to the flow

        Args:
            aws: list of callable or callable
        """
        if isinstance(aws, list):
            self.aws += [self.ensure_coroutine_func(a) for a in aws]
        else:
            self.aws.append(self.ensure_coroutine_func(aws))

        return self

    def __copy__(self) -> "AsyncFlow":
        """make a shallow copy"""
        aws = self.aws[:]
        args = self.args[:]
        kwargs = self.kwargs.copy()
        new_flow = self.__class__(*args, **kwargs)
        new_flow.aws = aws
        return new_flow

    async def run(self) -> Any:
        """run the flow

        Returns:
            coroutine
        """
        from aflowey.single_executor import SingleFlowExecutor

        return await SingleFlowExecutor(self).execute_flow()

    @staticmethod
    def log(log_str: str, print_arg: bool = False) -> Any:
        """utility function to log between steps, printing argument if needed"""

        async def wrapped(last_result: Any) -> Any:
            logger.info(log_str)
            if print_arg:  # pragma: no cover
                logger.info(last_result)
            return last_result

        return wrapped

    @staticmethod
    def from_args(*args: Any, **kwargs: Any) -> "AsyncFlow":
        """create a flow with given arguments as first input"""
        return AsyncFlow(*args, **kwargs)

    @staticmethod
    def empty() -> "AsyncFlow":
        """create an empty flow"""
        return AsyncFlow()

    @staticmethod
    def from_flow(flow: "AsyncFlow") -> "AsyncFlow":
        """create a new flow from given flow, copying it
        (args, kwargs, and aws functions)
        """
        assert isinstance(flow, AsyncFlow)
        return copy(flow)


# aliases
aflow = async_flow = AsyncFlow
flog = aflow.log
