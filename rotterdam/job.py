import json

from .exceptions import NoSuchJob


class Job(object):

    def __init__(self):
        self.when = None
        self.unique_key = None

        self.module = None
        self.function = None
        self.args = None
        self.kwargs = None

        self.call = None

    def serialize(self):
        return json.dumps({
            "when": self.when,
            "unique_key": self.unique_key,
            "module": self.module,
            "function": self.function,
            "args": self.args,
            "kwargs": self.kwargs
        })

    def deserialize(self, payload):
        payload = json.loads(payload)

        for attribute in [
            'when', 'unique_key', 'module', 'function', 'args', 'kwargs'
        ]:
            setattr(self, attribute, payload[attribute])

    def load(self):
        try:
            module = __import__(self.module, fromlist=self.function)
            self.call = getattr(module, self.function)
        except ImportError:
            raise NoSuchJob

    def run(self):
        return self.call(*self.args, **self.kwargs)

    def __repr__(self):
        arg_string = ", ".join(self.args)
        kwarg_string = ", ".join([
            "%s=%s" % (name, val)
            for name, val in self.kwargs.iteritems()
        ])
        if arg_string and kwarg_string:
            kwarg_string = ", " + kwarg_string
        return "%(module)s.%(func_name)s(%(args)s%(kwargs)s)" % {
            "module": self.module,
            "func_name": self.function,
            "args": arg_string,
            "kwargs": kwarg_string
        }
