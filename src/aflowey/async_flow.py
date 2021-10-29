from copy import copy
from typing import Any, Union, List, Dict, Optional

from aflowey import ensure_callable, F, async_wrap
from aflowey.f import FF
from aflowey.functions import is_side_effect, get_name, side_effect, named
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
        func = FF >> ensure_callable(func) >> async_wrap
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


def step(
    *func: Union[F, Function], name: Optional[str] = None, impure: bool = False
) -> Union[Function, List[F], F]:
    if len(func) > 1 and impure is False:
        raise ValueError("Can not have several functions for impure method")
    if impure:
        new_func = side_effect(*func)
        return new_func
    named_func = func[0]
    return named(named_func, name) if name is not None else named_func
