import errno
import json
import socket


SOCKET_BUFFER_SIZE = 4096


class Job(object):

    def __init__(self):
        self.spec = None
        self.args = None
        self.kwargs = None

    def serialize(self):
        return json.dumps({
            "spec": self.spec,
            "args": self.args,
            "kwargs": self.kwargs
        })

    def deserialize(self, payload):
        payload = json.loads(payload)

        self.spec = payload['spec']
        self.args = payload['args']
        self.kwargs = payload['kwargs']

    def __repr__(self):
        arg_string = ", ".join(self.args)
        kwarg_string = ", ".join([
            "%s=%s" % (name, val)
            for name, val in self.kwargs.iteritems()
        ])
        if arg_string and kwarg_string:
            kwarg_string = ", " + kwarg_string
        return "%(func_name)s(%(args)s%(kwargs)s)" % {
            "func_name": self.spec,
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
