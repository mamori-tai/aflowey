import unittest
from operator import attrgetter

from loguru import logger

from aflowey import aflow
from aflowey import async_exec
from aflowey import flog
from aflowey.f import breaker
from aflowey.f import F
from aflowey.f import F1
from aflowey.f import identity
from aflowey.f import impure
from aflowey.f import lift
from aflowey.f import spread


def x():
    return 1


def print_some_stuff():
    print("Hello from an async flow")


class Toto:
    def __init__(self):
        self.attribute = 0

    def print_some_stuff(self):
        print("hello from class instance")

    def print_x(self, x):
        print(x)

    def breaker(self, z):
        if z > 2:
            return aflow.CANCEL_FLOW
        return 1

    def return_new_value(self, z, x):
        print(x)
        return z + 1

    def spread(self, a, b):
        return a + b


def spread_function(x):
    return x, x + 1


class TestAsyncFlow(unittest.IsolatedAsyncioTestCase):
    simple_flow = aflow.empty() >> x
    impure_flow = aflow.from_flow(simple_flow) >> impure(
        print_some_stuff, print_some_stuff
    )

    async def test_flow_init(self):
        result = await TestAsyncFlow.impure_flow.run()
        self.assertEqual(result, 1)

    async def test_flow_exec_parallel(self):
        r1, r2 = await (
            async_exec(TestAsyncFlow.simple_flow) | TestAsyncFlow.impure_flow
        ).run()
        self.assertEqual(r1, 1)
        self.assertEqual(r2, 1)

    async def test_flow_exec_parallel_multiple(self):

        (r0, r1), r2 = await (
            async_exec([TestAsyncFlow.simple_flow, TestAsyncFlow.simple_flow])
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
            test.print_some_stuff
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

    async def test_lift(self):
        test = Toto()
        flow = aflow.from_flow(TestAsyncFlow.simple_flow) >> lift(
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

    async def test_lift_of_lift(self):
        async def async_toto(number, number_):
            return number + number_

        def toto(a_number, number, number_):
            print(a_number)
            return lift(async_toto, number=number, number_=number_)

        async def toto_async(a_number, number, number_):
            print(a_number)
            return lift(async_toto, number=number, number_=number_)

        func = lift(toto, number=4, number_=4)
        flow = aflow.from_flow(TestAsyncFlow.simple_flow) >> func
        result = await flow.run()
        self.assertEqual(result, 8)

        func2 = lift(toto_async, number=4, number_=4)
        flow = aflow.from_flow(TestAsyncFlow.simple_flow) >> func2
        result = await flow.run()
        self.assertEqual(result, 8)

    async def test_F1(self):
        def attr(x1):
            return x1

        flow = aflow.from_args(Toto()) >> F1(attr, extractor=attrgetter("attribute"))
        result = await flow.run()
        self.assertEqual(result, 0)

    async def test_chain_flow(self):
        def value(y):
            if y == 1:
                return aflow.CANCEL_FLOW
            return y

        def get_flow(z):
            return aflow.from_args(z) >> value

        flow = aflow.from_flow(TestAsyncFlow.simple_flow) >> breaker(get_flow)

        result = await flow.run()
        self.assertEqual(result, 1)
