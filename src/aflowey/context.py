from contextvars import ContextVar
from typing import Any, Callable

executor_var = ContextVar("executor", default=None)
ctx_var = ContextVar("flow executor context", default=None)


class Context:
    def __init__(self, **kwargs):
        self._context = dict(**kwargs)

    def set_value(self, key, value):
        self._context[key] = value

    def get_value(self, key):
        return self._context.get(key)

    @staticmethod
    def set(key, value):
        ctx = ctx_var.get()
        ctx.set_value(key, value)

    @staticmethod
    def get(key):
        ctx = ctx_var.get()
        return ctx.get_value(key)

    @staticmethod
    def get_and_set(key: str, fn: Callable[[Any], Any]) -> Any:
        value = Context.get(key)
        Context.set(key, fn(value))
        return value
