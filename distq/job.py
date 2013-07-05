import errno
import json
import socket


SOCKET_BUFFER_SIZE = 4096


class Job(object):

    def __init__(self):
        self.module = None
        self.function = None
        self.args = None
        self.kwargs = None

    def serialize(self):
        return json.dumps({
            "module": self.module,
            "function": self.function,
            "args": self.args,
            "kwargs": self.kwargs
        })

    def deserialize(self, payload):
        payload = json.loads(payload)

        for attribute in ['module', 'function', 'args', 'kwargs']:
            setattr(self, attribute, payload[attribute])

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


def job_iterator(connection):
        message = ''

        while True:
            try:
                chunk = connection.recv(SOCKET_BUFFER_SIZE)
                if chunk:
                    message = message + chunk
            except socket.error as e:
                if e.errno not in [errno.EAGAIN, errno.EINTR]:
                    raise

            if not message:
                break

            if "\n" in message:
                message, extra = message.split("\n", 1)

            job = Job()
            job.deserialize(message)

            yield job

            message = extra
