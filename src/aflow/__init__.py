"""Aflow."""
import asyncio
import functools
from inspect import iscoroutine, iscoroutinefunction, getfullargspec
from typing import Callable, Any

from loguru import logger

def identity(x):
    return x

class F:
    def __init__(self, func):
        self.func = func

    def __gt__(self, other):
        def _(*args, **kwargs):
            return other(self.func(*args, **kwargs))

        return F(_)

    def __call__(self, *args, **kwargs):
        res = self.func(*args, **kwargs)
        return res

    def __repr__(self):
        return f"<F instance: {self.func}"


def lift(func, *args, **kwargs) -> F:
    # func may return a lift
    new_func = functools.partial(func, *args, **kwargs)

    async def _(*a, **kw):
        # calling return lift function
        result = new_func(*a, **kw)
        if callable(result):
            result = result()
        if iscoroutine(result):
            result = await result
        return result

    return F(_)


def async_wrap(func):
    async def _(*args, **kwargs):
        result = func(*args, **kwargs)
        if iscoroutine(result):
            result = await result
        return result

    return _


class AsyncFlow:
    """

    flow = (Flow() >> gen1 >> gen2 >> gen3) | (Flow()
    await flow.run()
    """

    CANCEL_FLOW = object()

    def __init__(self, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs
        self.aws = []

        self.flows = []

        # to run in parallel
        self.other_flows = []

    def __ior__(self, flow: "AsyncFlow") -> "AsyncFlow":
        if not isinstance(flow, AsyncFlow):
            raise TypeError(f"Awaited AsyncFlow instance, got {type(flow).__name__}")
        self.aws.append(self.ensure_coroutine_func(flow))

        return self

    def __rshift__(self, aws) -> "AsyncFlow":
        if isinstance(aws, list):
            if not all(callable(a) for a in aws):
                raise TypeError(f"Expected list of callable, got {type(aws).__name__}")
            self.aws += [self.ensure_coroutine_func(a) for a in aws]
        else:
            if not callable(aws):
                raise TypeError(f"Expected callable, got {type(aws).__name__}")
            self.aws.append(self.ensure_coroutine_func(aws))

        return self

    def __or__(self, flow: "AsyncFlow") -> "AsyncFlow":
        if not isinstance(flow, AsyncFlow):
            raise ValueError(
                f"Expected an AsyncFlow instance, got {type(flow).__name__}"
            )
        self.other_flows.append(flow)

        return self

    @staticmethod
    def ensure_coroutine_func(func):
        side_effect_func = hasattr(func, "__side_effect__")
        if not iscoroutinefunction(func):
            func = async_wrap(func)
            if side_effect_func:
                AsyncFlow.side_effect(func)
            return func
        return func

    @staticmethod
    async def _exec_flow(flow: "Flow", *args, **kwargs) -> Any:
        if not flow.aws:
            logger.debug("no aws")
            return None
        current_args = await flow.aws[0](*args, **kwargs)
        if isinstance(current_args, AsyncFlow):
            current_args = await AsyncFlow._exec_flow(current_args, *current_args.args)

        for task in flow.aws[1:]:
            if hasattr(task, "__side_effect__"):
                result = await task(current_args)
                if result is AsyncFlow.CANCEL_FLOW:
                    logger.info("Received sentinel object, canceling flow...")
                    break
                continue

            current_args = await task(current_args)
            if isinstance(current_args, AsyncFlow):
                current_args = await AsyncFlow._exec_flow(
                    current_args, *current_args.args
                )

        # return current args that are the actual results
        return current_args

    async def run(self, **kwargs):
        f_exec = AsyncFlow._exec_flow

        # majority case
        if not self.flows and not self.other_flows:
            return await f_exec(self, *self.args, **self.kwargs)

        # flows are defined
        if not self.aws:
            self_exec = asyncio.gather(
                *[f_exec(flow, *flow.args, **flow.kwargs) for flow in self.flows]
            )
        else:
            # aws are defined
            self_exec = f_exec(self, *self.args, **self.kwargs)

        # if other flow run in parallel
        other_flows = [
            f_exec(flow, *flow.args, **flow.kwargs) for flow in self.other_flows
        ]

        # prepend previous flows
        other_flows = [self_exec, *other_flows]
        results = await asyncio.gather(*other_flows, **kwargs)
        return results

    @staticmethod
    def log(log_str, print_arg=False):
        async def _(last_result):
            logger.info(log_str)
            if print_arg:
                logger.info(last_result)
            return last_result

        return _

    @staticmethod
    def from_args(*args, **kwargs) -> "AsyncFlow":
        return AsyncFlow(*args, **kwargs)

    @staticmethod
    def empty() -> "AsyncFlow":
        return AsyncFlow()

    @staticmethod
    def from_flow(flow) -> "AsyncFlow":
        return flow

    @staticmethod
    def from_flows(flows) -> "AsyncFlow":
        flow = AsyncFlow()
        for f in flows:
            flow.flows.append(f)
        return flow

    @staticmethod
    def f1(func: Callable, extractor: Callable = None):
        def _(arg1):
            value = arg1 if extractor is None else extractor(arg1)
            return func(value)

        return F(_)

    @staticmethod
    def f0(func: Callable):
        def _(arg1):
            return func()

        return F(_)

    @staticmethod
    def fasync(func):
        return F(func) > async_wrap

    @staticmethod
    def may_fail(func):
        return func

    @staticmethod
    def spread_args(func):
        def _(args):
            return func(*args)

        return F(_)

    @staticmethod
    def side_effect(*func):
        def _(f):
            is_bound_method = hasattr(f, "__self__")
            # automatically create new function when the function
            # is a bound method and has no other input args
            f_args = getfullargspec(f).args
            if is_bound_method:
                if len(f_args) == 1 and f_args[0] == "self":
                    f = F0(f)
                else:
                    f = F(f)
            f.__side_effect__ = True

            return f

        return [_(fu) for fu in func]


# aliases
async_flow = AsyncFlow
aflow = async_flow
flog = aflow.log
_1 = F1 = aflow.f1
_0 = F0 = aflow.f0
imp = impure = aflow.side_effect
breaker = erratic = aflow.may_fail
partial = apartial = alift = lift
aident = aidentity = fidentity = async_wrap(identity)
spread = spread_args = aflow.spread_args
