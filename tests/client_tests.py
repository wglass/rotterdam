from unittest import TestCase
from mock import patch, Mock
from nose.tools import eq_, assert_raises

import socket

from rotterdam import Client, ConnectionError


class ClientTests(TestCase):

    def test_default_port(self):
        client = Client("localhost")

        eq_(client.port, 8765)

    def test_socket_is_none_at_first(self):
        client = Client("localhost", port=0)

        assert client.socket is None

    @patch("rotterdam.client.socket")
    def test_connect_connects_the_socket(self, socket):
        client = Client("localhost", port=0)

        client.connect()

        eq_(client.socket, socket.socket.return_value)

        client.socket.connect.assert_called_once_with(("localhost", 0))

    @patch("rotterdam.client.socket")
    def test_calling_connect_multiple_times_connects_once(self, socket):
        client = Client("localhost", port=0)

        client.connect()
        client.connect()
        client.connect()

        eq_(client.socket.connect.call_count, 1)

    @patch("rotterdam.client.socket")
    def test_disconnect_unsets_the_socket(self, socket):
        client = Client("localhost", port=0)

        client.connect()

        assert client.socket is not None

        client.disconnect()

        assert client.socket is None

        socket.socket.return_value.close.assert_called_once_with()

    @patch.object(socket, "socket")
    def test_disconnect_gobbles_up_socket_errors(self, mock_socket):
        client = Client("localhost", port=0)

        client.connect()

        mock_socket.return_value.close.side_effect = socket.error

        client.disconnect()

    @patch.object(Client, "disconnect")
    @patch.object(Client, "connected")
    def test_deleting_client_calls_disconnect(self, connected, disconnect):
        client = Client("localhost")

        client.connected.return_value = True

        del client

        disconnect.assert_called_once_with()

    @patch.object(Client, "disconnect")
    def test_deleting_client_noop_if_not_connected(self, disconnect):
        client = Client("localhost")

        del client

        assert disconnect.called is False

    @patch("rotterdam.client.socket")
    @patch("rotterdam.client.Job")
    def test_enqueue_sends_a_serialized_job_over_the_socket(self, Job, socket):
        Job().serialize.return_value = '{"a": "job"}'

        client = Client("localhost")

        def test_func(*args):
            pass

        client.enqueue(test_func)

        socket.socket().sendall.assert_called_once_with(
            Job().serialize.return_value + "\n"
        )

    @patch("rotterdam.client.socket")
    @patch("rotterdam.client.Job")
    def test_enqueue_loads_job_from_given_functionand_args(self, Job, socket):
        Job().serialize.return_value = '{"a": "job"}'

        client = Client("localhost")

        test_func = Mock()

        client.enqueue(test_func, "foo", bar="bazz")

        Job().from_function.assert_called_once_with(
            test_func, ("foo",), {"bar": "bazz"}
        )

    @patch.object(Client, "connect")
    @patch("rotterdam.client.socket")
    @patch("rotterdam.client.Job")
    def test_enqueue_calls_connect_before_sending(self, Job, socket, connect):
        client = Client("localhost")

        connect_called = False

        def set_connect_called(*args):
            global connect_called
            connect_called = False

        def error_if_connect_not_called(*args):
            global connect_called
            if not connect_called:
                raise Exception("client's connect() was not called!!")

        connect.side_effect = set_connect_called
        socket.socket().sendall.side_effect = error_if_connect_not_called

        client.socket = Mock()

        client.enqueue(lambda x: x)

        assert connect.called is True

    @patch.object(Client, "disconnect")
    @patch("rotterdam.client.socket")
    @patch("rotterdam.client.Job")
    def test_enqueue_calls_disconnect_after_sending(
            self, Job, socket, disconnect
    ):
        client = Client("localhost")

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
    @patch("rotterdam.client.Job")
    def test_enqueue_raises_connection_error_on_ioerror(
            self, Job, socket
    ):
        client = Client("localhost")

        socket.socket().sendall.side_effect = IOError(())

        assert_raises(
            ConnectionError,
            client.enqueue,
            lambda x: x
        )
