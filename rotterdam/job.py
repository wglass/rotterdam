import hashlib
import json
import types

from .exceptions import NoSuchJob, InvalidJobPayload


class Job(object):

    attributes = ['when', 'unique_key', 'module', 'function', 'args', 'kwargs']

    def __init__(self):
        for attribute in self.attributes:
            setattr(self, attribute, None)

        self.call = None

    def from_function(self, func, args, kwargs):
        self.args = args
        self.kwargs = kwargs

        if isinstance(func, basestring):
            self.module, self.function = func.split(":")
        elif isinstance(func, types.FunctionType):
            self.module = func.__module__
            self.function = func.__name__

        uniqueness = hashlib.md5()

        uniqueness.update(str(self.module))
        uniqueness.update(str(self.function))

        for arg in args:
            uniqueness.update(str(arg))
        for arg_name in sorted(kwargs.keys()):
            uniqueness.update(
                str(arg_name) + "=" + str(kwargs[arg_name])
            )

        self.unique_key = uniqueness.hexdigest()

        return self

    def from_payload(self, payload):
        try:
            json_payload = json.loads(payload)
        except ValueError:
            raise InvalidJobPayload

        for attribute in self.attributes:
            setattr(self, attribute, json_payload[attribute])

    def serialize(self):
        return json.dumps({
            "when": self.when,
            "unique_key": self.unique_key,
            "module": self.module,
            "function": self.function,
            "args": self.args,
            "kwargs": self.kwargs
        })

    def load(self):
        try:
            module = __import__(self.module, fromlist=self.function)
            self.call = getattr(module, self.function)
        except (ImportError, AttributeError):
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
