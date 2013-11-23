import errno
import json
import socket

from .exceptions import (
    InvalidJobPayload, NoSuchJob, ConnectionError, JobEnqueueError
)

SOCKET_BUFFER_SIZE = 4096


class Client(object):

    def __init__(self, host, port=8765):
        self.host = host
        self.port = port

        self.socket = None

    @property
    def connected(self):
        return self.socket and self.socket.fileno()

    def connect(self):
        if self.connected:
            return

        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.connect((self.host, self.port))

    def enqueue(self, func, *args, **kwargs):
        payload = {
            "args": args,
            "kwargs": kwargs
        }

        if isinstance(func, basestring):
            module, func = func.split(":")
        else:
            module = func.__module__
            func = func.__name__

        payload['module'] = module
        payload['func'] = func

        self.connect()

        try:
            self.socket.sendall(json.dumps(payload) + "\n")
        except IOError, e:
            raise ConnectionError(
                "Error sending job to %s:%s, %s" % (
                    self.host,
                    self.port,
                    e.args[1] if len(e.args) > 1 else e.args[0]
                )
            )
            self.disconnect()

        response = ''
        while True:
            try:
                chunk = self.socket.recv(SOCKET_BUFFER_SIZE)
                if chunk:
                    response = response + chunk
            except socket.error as e:
                if e.errno not in [errno.EAGAIN, errno.EINTR]:
                    raise

            if not response or "\n" in response:
                break

        if not response:
            raise JobEnqueueError("empty response")

        response = json.loads(response)

        if response['status'] != "ok":
            if response["message"] == "no such job":
                raise NoSuchJob
            elif response["message"] == "invalid payload":
                raise InvalidJobPayload
            else:
                raise JobEnqueueError(response["message"])

        self.disconnect()

    def disconnect(self):
        try:
            self.socket.close()
        except socket.error:
            pass
        self.socket = None

    def __del__(self):
        if self.connected:
            self.disconnect()
