import hashlib
import json
import logging
import os
import random
import time

from .serialization import DateAwareJSONDecoder, DateAwareJSONEncoder
from .exceptions import NoSuchJob, InvalidPayload


class Payload(object):

    def __init__(self):
        self.module = None
        self.func = None
        self.args = None
        self.kwargs = None
        self.call = None

        self.when = None

        self.unique_key = None

        self.logger = None

    @classmethod
    def deserialize(cls, message):
        instance = cls()

        instance.logger = logging.getLogger(__name__)

        try:
            payload = json.loads(message, cls=DateAwareJSONDecoder)
        except ValueError:
            instance.logger.exception("Error when loading json")
            raise InvalidPayload

        instance.module = payload['module']
        instance.func = payload['func']
        instance.args = payload.get('args', [])
        instance.kwargs = payload.get('kwargs', {})

        instance.unique_key = payload.get("unique_key")
        instance.when = payload.get("when", time.time())

        try:
            module = __import__(payload['module'], fromlist=payload['func'])
            instance.call = getattr(module, instance.func)
        except (KeyError, ImportError, AttributeError):
            raise NoSuchJob

        metadata = instance.call.job_metadata

        instance.queue_name = metadata['queue_name']
        if metadata['delay']:
            instance.when += metadata['delay'].total_seconds()

        if instance.unique_key:
            return instance

        uniques = [instance.module, instance.func, instance.queue_name]
        if instance.args:
            uniques.extend(instance.args)
        if instance.kwargs:
            uniques.extend([
                name + "=" + str(value)
                for name, value in instance.kwargs.iteritems()
            ])
        if not metadata['unique']:
            uniques += [time.time(), os.getpid(), random.random()]

        uniqueness = hashlib.md5()
        for unique in uniques:
            uniqueness.update(str(unique))

        instance.unique_key = uniqueness.hexdigest()

        return instance

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
