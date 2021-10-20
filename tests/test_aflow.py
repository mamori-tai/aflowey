import unittest

from aflow import aflow
from aflow.f import impure, F1, F0


def x():
    return 1


def print_some_stuff():
    print("Hello from an async flow")


class TestAsyncFlow(unittest.IsolatedAsyncioTestCase):
    simple_flow = aflow.empty() >> x
    impure_flow = aflow.from_flow(simple_flow) >> impure(print_some_stuff)

    async def test_flow_init(self):
        ...

        result = await TestAsyncFlow.impure_flow.run()
        self.assertEqual(result, 1)
