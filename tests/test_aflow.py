import asyncio
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor

from aflowey.context import ctx_var, Context

try:
    from unittest import IsolatedAsyncioTestCase
except ImportError:
    from aiounittest import AsyncTestCase as IsolatedAsyncioTestCase
from operator import attrgetter
from typing import Any
from typing import Callable
from typing import cast

from loguru import logger

from aflowey import aexec
from aflowey import aflow
from aflowey import async_exec
from aflowey import CANCEL_FLOW
from aflowey import flog
from aflowey.async_flow import step as _
from aflowey.executor import flows_from_arg
from aflowey.executor import run_flows
from aflowey.f import F
from aflowey.f import FF
from aflowey.functions import breaker
from aflowey.functions import F0
from aflowey.functions import F1
from aflowey.functions import identity
from aflowey.functions import impure
from aflowey.functions import lift
from aflowey.functions import make_impure
from aflowey.functions import named
from aflowey.functions import partial
from aflowey.functions import spread
from aflowey.functions import spread_kw


def x():
    return 1


def print_some_stuff():
    print("Hello from an async flow")


def print_x():
    logger.debug("Hello")


class Toto:
    def __init__(self):
        self.attribute = 0

    def print_some_stuff(self):
        print("hello from class instance")

    def print_x(self, x):
        print(x)

    def breaker(self, z):
        if z > 2:
            return CANCEL_FLOW
        return 1

    def return_new_value(self, z, x):
        print(x)
        return z + 1

    def spread(self, a, b):
        return a + b


def spread_function(x):
    return x, x + 1


class TestAsyncFlow(IsolatedAsyncioTestCase):
    simple_flow = aflow.empty() >> x
    impure_flow = aflow.from_flow(simple_flow) >> impure(
        F0(print_some_stuff), F0(print_some_stuff)
    )

    async def test_flow_init(self):
        result = await TestAsyncFlow.impure_flow.run()
        self.assertEqual(result, 1)

    async def test_flow_exec_parallel(self):
        r1, r2 = await (
            async_exec().from_flows(TestAsyncFlow.simple_flow)
            | TestAsyncFlow.impure_flow
        ).run()
        self.assertEqual(r1, 1)
        self.assertEqual(r2, 1)

    async def test_flow_exec_parallel_multiple(self):
        (r0, r1), r2 = await (
            async_exec().from_flows(
                [TestAsyncFlow.simple_flow, TestAsyncFlow.simple_flow]
            )
            | TestAsyncFlow.impure_flow
        ).run()
        self.assertEqual(r0, 1)
        self.assertEqual(r1, 1)
        self.assertEqual(r2, 1)

    async def test_flow_ensure_callable(self):
        flow = aflow.from_flow(TestAsyncFlow.simple_flow) >> 4
        result = await flow.run()
        self.assertEqual(result, 4)

    async def test_impure_from_instance(self):
        test = Toto()
        flow = aflow.from_flow(TestAsyncFlow.simple_flow) >> impure(
            F0(test.print_some_stuff)
        )
        await flow.run()

        flow = aflow.from_flow(TestAsyncFlow.simple_flow) >> impure(test.print_x)
        await flow.run()

    async def test_break(self):
        test = Toto()

        flow = aflow.from_flow(TestAsyncFlow.simple_flow) >> 4 >> breaker(test.breaker)
        result = await flow.run()
        self.assertEqual(result, 4)

        flow = aflow.from_flow(TestAsyncFlow.simple_flow) >> 1 >> breaker(test.breaker)
        result = await flow.run()
        self.assertEqual(result, 1)

    async def test_partial(self):
        test = Toto()
        flow = aflow.from_flow(TestAsyncFlow.simple_flow) >> partial(
            test.return_new_value, x=12
        )
        self.assertEqual(await flow.run(), 2)

    async def test_spread_function(self):
        test = Toto()
        flow = (
            aflow.from_flow(TestAsyncFlow.simple_flow)
            >> spread_function
            >> spread(test.spread)
        )
        self.assertEqual(await flow.run(), 3)

    async def test_from_args(self):
        flow = aflow.from_args(1) >> identity >> flog("End of the flow", print_arg=True)
        result = await flow.run()
        self.assertEqual(result, 1)

    async def test_empty_flow(self):
        empty = aflow.empty()
        result = await empty.run()
        self.assertIsNone(result)

    async def test_func_composition(self):
        def add(x):
            return x + 1

        new_func = F(identity) > add
        result = new_func(1)
        self.assertEqual(result, 2)

    async def test_repr(self):
        self.assertTrue("identity" in repr(F(identity)))

    async def test_async_flow(self):
        async def toto(a_number):
            return a_number + 12

        flow = aflow.from_flow(TestAsyncFlow.simple_flow) >> toto
        result = await flow.run()
        self.assertEqual(result, 13)

    async def test_partial_of_partial(self):
        async def async_toto(number, number_):
            return number + number_

        async def toto(a_number, number, number_):
            return partial(async_toto, number=number, number_=number_)

        async def toto_async(a_number, number, number_):
            return partial(async_toto, number=number, number_=number_)

        async def tata(a_num):
            return partial(toto, a_number=a_num, number=1, number_=1)

        func = partial(toto, number=4, number_=4)
        flow = aflow.from_flow(TestAsyncFlow.simple_flow) >> func
        result = await flow.run()
        self.assertEqual(result, 8)

        func2 = partial(toto_async, number=4, number_=4)
        flow = aflow.from_flow(TestAsyncFlow.simple_flow) >> func2
        result = await flow.run()
        self.assertEqual(result, 8)

        func3 = partial(tata)
        flow = aflow.from_flow(TestAsyncFlow.simple_flow) >> func3
        result = await flow.run()
        logger.debug(result)

    async def test_F1(self):
        def attr(x1):
            return x1

        flow = aflow.from_args(Toto()) >> F1(attr, extractor=attrgetter("attribute"))
        result = await flow.run()
        self.assertEqual(result, 0)

    async def test_chain_flow(self):
        def value(y):
            if y == 1:
                return CANCEL_FLOW
            return y

        def get_flow(z):
            return aflow.from_args(z) >> value

        flow = aflow.from_flow(TestAsyncFlow.simple_flow) >> breaker(get_flow)

        result = await flow.run()
        self.assertEqual(result, 1)

    async def test_early_breaker(self):
        def value(y):
            if y == 1:
                return CANCEL_FLOW
            return y

        def get_flow(z):
            return aflow.from_args(z) >> value

        flow = aflow.from_args(1) >> breaker(get_flow)

        result = await flow.run()
        self.assertEqual(result, 1)

    async def test_flow_with_impure_bound_method(self):
        class MyClass:
            async def bound_method(self):
                await asyncio.sleep(0.1)

        a = MyClass()
        f = aflow.from_args(1) >> impure(F0(a.bound_method))
        r = await f.run()
        self.assertEqual(r, 1)

        f = aflow.empty() >> impure(F0(a.bound_method))
        r = await f.run()
        self.assertEqual(r, None)

        f = aflow.empty() >> a.bound_method
        r = await f.run()
        self.assertEqual(r, None)

    async def test_flow_with_step_name(self):
        toto = Toto()
        f = (
            aflow.empty()
            >> named(1, "first_step")
            >> named(impure(toto.print_x), "second_step")
        )
        await f.run()
        logger.debug(f.steps)

    async def test_kw_flow(self):
        def check_data(a, b, c):
            print(a, b, c)

        f = (
            aflow.from_args(a=1, b=2, c=3)
            >> ((F(check_data) >> spread_kw) >> make_impure)
            # >> impure(spread_kw(check_data))
        )
        await f.run()

    async def test_workflow_no_f(self):
        class MyClass:
            def print_a(self):
                print(1)

        a = MyClass()
        await (aflow.empty() >> a.print_a).run()

    async def test_aexec_multiple_aws(self):
        async def print_a_value(y):
            await asyncio.sleep(0.01)
            return y

        ((a, b, c),) = (
            await aexec()
            .from_flows(
                [
                    partial(print_a_value, 1),
                    partial(print_a_value, 2),
                    partial(print_a_value, 3),
                ]
            )
            .run()
        )
        self.assertEqual((a, b, c), (1, 2, 3))

    async def test_flow_step(self):
        print_stuff = FF >> print_some_stuff >> F0
        flow = (
            aflow.empty()
            >> _(x, name="first step")
            >> _(identity, name="second step")
            >> _(print_stuff, name="impure", impure=True)
            >> _(lambda v: (v + 1, v + 2), name="add_one")
            >> _(spread(lambda v, z: (z**2, v**2)), name="pow2")
        )

        result = await flow.run()
        logger.debug(result)
        self.assertEqual(result, (9, 4))
        logger.debug(flow.steps)

    async def test_multiple_flows(self):
        def pow_(z, y):
            return z**y

        def get_flow(x):
            return aflow.from_args(x) >> identity

        flow = (
            aflow.empty()
            >> _(x, name="first_step")
            >> flows_from_arg(partial(pow_, 1), partial(pow_, 2), get_flow)
            >> flog(print_arg=True)
        )
        self.assertEqual(await flow.run(), [1, 2, 1])

    async def test_with_statement(self):
        def pow_(z, y):
            return z**y

        def get_flow(y):
            return aflow.from_args(y) >> identity

        flow = (
            aflow.empty()
            >> _(x, name="first_step")
            >> run_flows(partial(pow_, 1), partial(pow_, 2), get_flow)
            >> flog(print_arg=True)
        )

        with (aexec() | flow).thread_runner() as runner:
            ((a, b, c),) = await runner.run()
            self.assertEqual((a, b, c), (1, 2, 1))

    async def test_lift_2(self):
        def z(value):
            return value * 2

        func = lift(z)
        self.assertEqual(list(func([1, 2, 3])), [2, 4, 6])

        def zz(value):
            return 1 / value

        from fn.monad import Option

        lifted_zz = cast(Callable[[Any], Option], lift(zz, lift_op=Option.from_call))
        self.assertEqual(lifted_zz(1).get_or(0), 1)

        self.assertEqual(lifted_zz(0).get_or(10000), 10000)

    async def test_context(self):

        ctx = dict(name="Marco")

        async def z():
            await asyncio.sleep(0.1)
            value = ctx["name"]
            ctx["name"] = value.upper()

        async def zz():
            await asyncio.sleep(0.1)
            return ctx.get("name") + "_checked"

        flow = aflow.empty() >> z >> (FF >> zz >> F0)
        self.assertEqual(await flow.run(), "MARCO_checked")

    async def test_context_2(self):
        async def a():
            await asyncio.sleep(0.1)
            logger.debug(Context.get_and_set("toto", lambda z: z.upper()))

        def aa():
            logger.debug(Context.get_and_set("toto", lambda z: z + "_checked"))

        def aaa():
            logger.debug(Context.get_and_set("toto", lambda z: z + "_unbelievable"))

        async def aaaa():
            await asyncio.sleep(0.1)
            logger.debug(Context.get("toto"))

        flow = (
            aflow.empty()
            >> a
            >> (FF >> aa >> F0)
            >> (FF >> aaa >> F0)
            >> (FF >> aaaa >> F0)
        )
        pflow = aexec() | flow
        with pflow.thread_runner() as runner:
            self.assertEqual(await runner.run(context={"toto": "Mathieu"}), [None])

    async def test_test_test(self):
        test = aexec() | self.simple_flow | self.impure_flow
        with test.thread_runner() as runner:
            self.assertEqual(await runner.run(), [1, 1])
        self.assertEqual(ctx_var.get(), None)

    async def test_test_process_pool(self):

        test = aexec() | self.simple_flow | impure(print_x)
        with test.process_runner() as runner:
            self.assertEqual(await runner.run(), [1, None])
        self.assertEqual(ctx_var.get(), None)

    async def test_test_process_pool_2(self):

        test = aexec() | self.simple_flow | impure(print_x)
        with test.with_process_runner(ProcessPoolExecutor()) as runner:
            self.assertEqual(await runner.run(), [1, None])
        self.assertEqual(ctx_var.get(), None)

        test = aexec() | self.simple_flow | impure(print_x)
        with test.with_thread_runner(ThreadPoolExecutor()) as runner:
            self.assertEqual(await runner.run(), [1, None])
        self.assertEqual(ctx_var.get(), None)

    async def test_multiple_early_return(self):
        def print_all(x, y, z):
            logger.debug("{}, {}, {}", x, y, z)

        result = await (aflow.from_args(1, 2, 3) >> impure(print_all)).run()
        self.assertEqual(result, (1, 2, 3))

    async def test_check_error(self):
        def test_error():
            raise ValueError()

        result = aflow.empty() >> test_error

        with self.assertRaises(ValueError):
            await result.run(return_exceptions=False)

        self.assertEqual(result.is_success, False)
        self.assertEqual(result.executed, True)

        #
        await result.run(return_exceptions=True)
        self.assertEqual(result.is_success, False)
        self.assertEqual(result.executed, True)

    async def test_return_exceptions_when_multiple_values(self):
        def test_error():
            raise ValueError("Unknown error")

        flow1 = aflow.empty() >> test_error
        flow2 = aflow.empty() >> 1
        runner = aexec.empty() | flow1 | flow2
        r1, r2 = await runner.run(return_exceptions=True)
        self.assertIsInstance(r1, ValueError)
        self.assertEqual(r2, 1)
        self.assertEqual(flow1.is_success, False)
        self.assertEqual(flow1.executed, True)
        self.assertEqual(flow2.is_success, True)
        self.assertEqual(flow2.executed, True)

    async def test_flow_str(self):
        print(type(self.simple_flow))
        f = aflow.empty() >> impure(lambda: 1) >> self.simple_flow
        logger.debug("HELLO {}", type(f.aws[1].func))
        self.assertEqual(await f.run(), 1)
        f.display()
