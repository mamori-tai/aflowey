from contextvars import ContextVar

executor_var = ContextVar("executor", default=None)
ctx_var = ContextVar("flow executor context", default=None)


class Context:

    def __init__(self, **kwargs):
        self._context = dict(**kwargs)

    async def set_value(self, key, value):
        self._context[key] = value

    def get_value(self, key):
        return self._context.get(key)

    @staticmethod
    async def set(key, value):
        ctx = ctx_var.get()
        await ctx.set_value(key, value)

    @staticmethod
    def get(key):
        ctx = ctx_var.get()
        return ctx.get_value(key)

