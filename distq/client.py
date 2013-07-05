import json
import socket
import types


class Client(object):

    def __init__(self, address):
        self.address = address
        self.socket = None

    def connect(self):
        if self.socket:
            return

        sock = socket.socket(socket.AF_UNIX)
        sock.connect(self.address)

        self.socket = sock

    @property
    def is_connected(self):
        return bool(self.socket is not None)

    def enqueue(self, func, *args, **kwargs):
        if not self.is_connected:
            self.connect()

        if isinstance(func, basestring):
            module, function = func.split(":")
        elif isinstance(func, types.FunctionType):
            module = func.__module__,
            function = func.__name__

        self.socket.sendall(json.dumps({
            "module": module,
            "function": function,
            "args": args,
            "kwargs": kwargs
        }) + "\n")
