import json
import socket


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

        self.socket.sendall(json.dumps({
            "spec": func.__module__ + "." + func.func_name,
            "args": args,
            "kwargs": kwargs
        }) + "\n")
