from copy import copy
from typing import Any
from typing import Dict
from typing import List
from typing import Optional
from typing import Union

from aflowey.functions import ensure_callable
from aflowey.f import F
from aflowey.functions import ensure_f
from aflowey.functions import named
from aflowey.functions import side_effect
from aflowey.types import Executor
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
    def ensure_f_function(func: Union[Function, F]) -> F:
        new_func: Function = ensure_callable(func)
        return ensure_f(new_func)

    def __rshift__(
        self, aws: Union[List[Union[F, Function]], Union[F, Function]]
    ) -> "AsyncFlow":
        """add a new step to the flow

        Args:
            aws: list of callable or callable
        """
        if isinstance(aws, list):
            self.aws += [self.ensure_f_function(a) for a in aws]
        else:
            self.aws.append(self.ensure_f_function(aws))

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
        raise ValueError(
            "Can not have several functions for impure method"
        )  # pragma: no cover
    if impure:
        new_func = side_effect(*func)
        return new_func
    named_func = func[0]
    return named(named_func, name) if name is not None else named_func
