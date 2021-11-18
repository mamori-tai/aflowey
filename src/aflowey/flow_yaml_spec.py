from typing import Callable, Any, Mapping, Awaitable, Optional

from fn.iters import first
from yaml import safe_load

from aflowey import impure, spread, F0, F1, spread_kw, partial
from aflowey.async_flow import AsyncFlow, aflow
from aflowey.executor import ExecutorType, aexec, run_flows


class YamlFlow:
    """Create flow from a yaml file

    >>> yaml_flow = (
    >>>    YamlFlow()
    >>>     .register(flow)
    >>>     .register(flow2)
    >>> )
    >>> yaml_flow.validate(yaml_file)
    >>> await yaml_flow.run()
    """

    FUNCTORS_FUNC = {
        "spread": spread,
        "F0": F0,
        "F1": F1,
        "spread_kw": spread_kw,
        "partial": partial
    }

    FUNCTION = "function"
    FUNCTOR = "functor"
    STEP = "step"
    EVAL = "eval"
    PARALLEL_FLOWS = "parallel-flows"
    ARGS = "args"
    KWARGS = "kwargs"
    IMPURE = "impure"
    EXECUTOR = "executor"

    def __init__(self):
        self.registered_flows = {}
        self.yaml_dict = None

    @staticmethod
    def _parse(yaml: str) -> Mapping[str, Any]:
        return safe_load(yaml)

    def _validate(self) -> None:
        if not self.yaml_dict:
            raise ValueError("Got an empty definition file")

        # check defined function in yaml to be present
        function_names = set(self.registered_flows.keys())
        for step in self.steps:
            step_dict = step[self.STEP]
            if self.PARALLEL_FLOWS in step_dict:
                continue
            function = step_dict[self.FUNCTION]
            if isinstance(function, dict):
                continue
            if function not in function_names:
                raise ValueError(f"used function has not been registered: {function}")

    def validate(self, yaml_flow: Mapping[str, Any]) -> None:
        self.yaml_dict = self._parse(yaml_flow)
        self._validate()

    def register(self, func: Callable[[Any], AsyncFlow], name: Optional[str] = None) -> 'YamlFlow':
        """register a function which produces an async flow"""
        key = func.__name__ if name is None else name
        self.registered_flows[key] = func
        return self

    def _handle_step(self, step: Mapping[str, Any]):
        func_name_or_dict = step[self.FUNCTION]

        # retrieve the function
        if isinstance(func_name_or_dict, dict):
            # may raise a key error
            func = eval(func_name_or_dict[self.EVAL], globals(), locals())
        else:
            func = self.registered_flows[func_name_or_dict]

        if "call" in step:
            if step["call"]:
                args, kwargs = [], {}
                if self.ARGS in step:
                    args = step[self.ARGS]
                if self.KWARGS in step:
                    kwargs = step[self.KWARGS]

                func = func(*args, **kwargs)

        # handle functor
        if self.FUNCTOR in step:
            functor_name = step[self.FUNCTOR]
            try:
                functor_func = YamlFlow.FUNCTORS_FUNC[functor_name]
            except KeyError as e:
                raise ValueError("Functor not known") from e
            else:
                # check args and kwargs
                if functor_name == 'partial':
                    args, kwargs = [], {}
                    if self.ARGS in step:
                        args = step[self.ARGS]
                    if self.KWARGS in step:
                        kwargs = step[self.KWARGS]
                    func = functor_func(func, *args)  # , **kwargs)
                else:
                    func = functor_func(func)
        # make the function impure of needed
        if self.IMPURE in step:
            is_impure = step[self.IMPURE]
            if is_impure:
                func = impure(func)
        return func

    def _transform_func(self, step: Mapping[str, Any]) -> Callable[[Any], Any]:
        if self.PARALLEL_FLOWS in step:
            # handle parallel_flows list of flows
            flows = step[self.PARALLEL_FLOWS]
            return run_flows(*[self._handle_step(flow["flow"]) for flow in flows])
        return self._handle_step(step)

    @staticmethod
    def _get_executor_type(executor: str) -> ExecutorType:
        if executor == 'thread':
            return ExecutorType.THREAD_POOL
        elif executor == 'process':
            return ExecutorType.PROCESS_POOL

    @property
    def steps(self):
        return (step for step in self.yaml_dict["flow"] if self.STEP in step)

    async def run(self) -> Awaitable[Any]:
        flow = aflow.empty()
        for step in self.steps:
            computed_func = self._transform_func(step[self.STEP])
            flow >> computed_func
        yaml_flow = self.yaml_dict["flow"]
        executor_step = first(element for element in yaml_flow if self.EXECUTOR in element)
        if executor_step:
            with aexec(self._get_executor_type(executor_step[self.EXECUTOR])) as executor:
                return await executor.from_flows(flow).run()
        return flow.run()
