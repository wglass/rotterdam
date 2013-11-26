import hashlib
import json
import logging
import os
import random
import time

from .serialization import DateAwareJSONDecoder, DateAwareJSONEncoder
from .exceptions import NoSuchJob, InvalidPayload


class Payload(object):

    def __init__(self, message):
        self.logger = logging.getLogger(__name__)

        try:
            payload = json.loads(message, cls=DateAwareJSONDecoder)
        except ValueError:
            self.logger.exception("Error when loading json")
            raise InvalidPayload

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
        uniques.extend(self.args)
        uniques.extend([
            name + "=" + value
            for name, value in self.kwargs
        ])
        if not metadata['unique']:
            uniques += [time.time(), os.getpid(), random.random()]

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
        }, cls=DateAwareJSONEncoder)

    def run(self):
        return self.call(*self.args, **self.kwargs)

    def __repr__(self):
        arg_string = ", ".join([str(arg) for arg in self.args])
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
