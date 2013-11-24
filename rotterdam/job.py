import hashlib
import json
import os
import random
import time

from .exceptions import NoSuchJob, InvalidJobPayload


class Job(object):

    def __init__(self, payload):
        try:
            payload = json.loads(payload)
        except ValueError:
            raise InvalidJobPayload

        self.module = payload['module']
        self.func = payload['func']
        self.args = payload.get('args', [])
        self.kwargs = payload.get('kwargs', {})

        self.unique_key = payload.get("unique_key")
        self.when = payload.get("when", time.time())

        try:
            module = __import__(payload['module'], fromlist=payload['func'])
            self.call = getattr(module, self.func)
        except (KeyError, ImportError, AttributeError):
            raise NoSuchJob

        metadata = self.call.job_metadata

        self.queue = metadata['queue']
        if metadata['delay']:
            self.when += metadata['delay'].total_seconds()

        if self.unique_key:
            return

        uniques = [self.module, self.func, self.queue]

        if not metadata['unique']:
            uniques += [time.time(), os.getpid(), random.random()]

        for arg_index, arg_name in enumerate(sorted(metadata['arg_names'])):
            if (
                    metadata['unique'] not in [True, False]
                    and arg_name not in metadata['unique']
            ):
                continue

            if arg_name in self.kwargs:
                uniques.append(arg_name + "=" + self.kwargs[arg_name])
            else:
                uniques.append(self.args[arg_index])

        uniqueness = hashlib.md5()
        for unique in uniques:
            uniqueness.update(str(unique))

        self.unique_key = uniqueness.hexdigest()

    def serialize(self):
        return json.dumps({
            'when': self.when,
            'unique_key': self.unique_key,
            'module': self.module,
            'func': self.func,
            'args': self.args,
            'kwargs': self.kwargs
        })

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
        return "%(module)s.%(func)s(%(args)s%(kwargs)s)" % {
            "module": self.module,
            "func": self.func,
            "args": arg_string,
            "kwargs": kwarg_string
        }
