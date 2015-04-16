import hashlib
import json
import logging
import os
import random
import time

from .serialization import DateAwareJSONDecoder, DateAwareJSONEncoder
from .exceptions import NoSuchJob, InvalidPayload


logger = logging.getLogger(__name__)


class Payload(object):

    def __init__(self):
        self.module = None
        self.func = None
        self.args = None
        self.kwargs = None
        self.call = None

        self.when = None

        self.unique_key = None

    @classmethod
    def deserialize(cls, message):
        try:
            payload = json.loads(message, cls=DateAwareJSONDecoder)
        except ValueError:
            logger.exception("Error when loading json")
            raise InvalidPayload

        instance = cls.import_func(payload['module'], payload['func'])
        instance.module = payload['module']
        instance.func = payload['func']
        instance.args = payload.get('args', [])
        instance.kwargs = payload.get('kwargs', {})
        instance.unique_key = payload.get("unique_key")
        instance.when = payload.get("when", time.time())

        metadata = instance.call.job_metadata
        instance.queue_name = metadata['queue_name']
        if metadata['delay']:
            instance.when += metadata['delay'].total_seconds()

        if not instance.unique_key:
            instance.determine_unique_key()

        return instance

    @classmethod
    def import_func(cls, module, func):
        instance = cls()
        try:
            module = __import__(module, fromlist=func)
            instance.call = getattr(module, func)
        except (KeyError, ImportError, AttributeError):
            raise NoSuchJob

        return instance

    def determine_unique_key(self):
        uniques = [self.module, self.func, self.queue_name]
        if self.args:
            uniques.extend(self.args)
        if self.kwargs:
            uniques.extend([
                name + "=" + str(value)
                for name, value in self.kwargs.iteritems()
            ])
        if not self.call.job_metadata['unique']:
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
