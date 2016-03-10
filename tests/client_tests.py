from unittest import TestCase
from mock import patch
from nose.tools import eq_, assert_raises

import json
import socket

from rotterdam import Rotterdam, ConnectionError


class ClientTests(TestCase):

    def test_default_port(self):
        client = Rotterdam("localhost")

        eq_(client.port, 8765)

    def test_socket_is_none_at_first(self):
        client = Rotterdam("localhost", port=0)

        assert client.socket is None

    @patch("rotterdam.client.socket")
    def test_connect_connects_the_socket(self, socket):
        client = Rotterdam("localhost", port=0)

        client.connect()

        eq_(client.socket, socket.socket.return_value)

        client.socket.connect.assert_called_once_with(("localhost", 0))

    @patch("rotterdam.client.socket")
    def test_calling_connect_multiple_times_connects_once(self, socket):
        client = Rotterdam("localhost", port=0)

        client.connect()
        client.connect()
        client.connect()

        eq_(client.socket.connect.call_count, 1)

    @patch("rotterdam.client.socket")
    def test_disconnect_unsets_the_socket(self, socket):
        client = Rotterdam("localhost", port=0)

        client.connect()

        assert client.socket is not None

        client.disconnect()

        assert client.socket is None

        socket.socket.return_value.close.assert_called_once_with()

    @patch.object(socket, "socket")
    def test_disconnect_gobbles_up_socket_errors(self, mock_socket):
        client = Rotterdam("localhost", port=0)

        client.connect()

        mock_socket.return_value.close.side_effect = socket.error

        client.disconnect()

    @patch.object(Rotterdam, "disconnect")
    @patch.object(Rotterdam, "connected")
    def test_deleting_client_calls_disconnect(self, connected, disconnect):
        client = Rotterdam("localhost")

        client.connected.return_value = True

        del client

        disconnect.assert_called_once_with()

    @patch.object(Rotterdam, "disconnect")
    def test_deleting_client_noop_if_not_connected(self, disconnect):
        client = Rotterdam("localhost")

        del client

        assert disconnect.called is False

    @patch("rotterdam.client.socket")
    def test_enqueue_sends_a_simple_payload_over_the_socket(self, socket):
        socket.socket().recv.return_value = '{"status": "ok"}\n'

        client = Rotterdam("localhost")

        def test_func(*args):
            pass

        client.enqueue(test_func)

        sendall = socket.socket().sendall

        eq_(sendall.call_count, 1)

        sent_payload = sendall.call_args[0][0]

        eq_(
            json.loads(sent_payload),
            {
                "func": "test_func",
                "module": __name__,
                "args": [],
                "kwargs": {}
            }
        )

        assert sent_payload.endswith("\n")

    @patch("rotterdam.client.socket")
    def test_enqueue_can_send_args_and_kwargs(self, socket):
        socket.socket().recv.return_value = '{"status": "ok"}\n'

        client = Rotterdam("localhost")

        def test_func(*args):
            pass

        client.enqueue(test_func, "foo", bar="bazz")

        sendall = socket.socket().sendall

        eq_(sendall.call_count, 1)

        sent_payload = sendall.call_args[0][0]

        eq_(
            json.loads(sent_payload),
            {
                "func": "test_func",
                "module": __name__,
                "args": ["foo"],
                "kwargs": {"bar": "bazz"}
            }
        )

    @patch.object(Rotterdam, "connect")
    @patch("rotterdam.client.socket")
    def test_enqueue_calls_connect_before_sending(self, socket, connect):
        socket.socket().recv.return_value = '{"status": "ok"}\n'

        client = Rotterdam("localhost")

        assert connect.called is False

        def test_func(*args):
            pass

        def error_if_connect_not_called(*args):
            if not connect.called:
                raise Exception("client's connect() was not called!!")

        socket.socket().sendall.side_effect = error_if_connect_not_called

        client.socket = socket.socket()

        client.enqueue(test_func)

        assert connect.called is True
        assert socket.socket().sendall.called is True

    @patch.object(Rotterdam, "disconnect")
    @patch("rotterdam.client.socket")
    def test_enqueue_calls_disconnect_after_sending(self, socket, disconnect):
        socket.socket().recv.return_value = '{"status": "ok"}\n'

        client = Rotterdam("localhost")

        disconnect_called = False

        def set_disconnect_called(*args):
            global disconnect_called
            disconnect_called = True

        def error_if_disconnect_called(*args):
            if disconnect_called:
                raise Exception("client's disconnect() was called!!")

        disconnect.side_effect = set_disconnect_called
        socket.socket().sendall.side_effect = error_if_disconnect_called

        client.enqueue(lambda x: x)

        assert disconnect.called is True

    @patch("rotterdam.client.socket")
    def test_enqueue_raises_connection_error_on_ioerror(self, socket):
        client = Rotterdam("localhost")

        socket.socket().sendall.side_effect = IOError(())

        assert_raises(
            ConnectionError,
            client.enqueue,
            lambda x: x
        )
