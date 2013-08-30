from unittest import TestCase
from mock import patch
from nose.tools import eq_

from rotterdam import Client


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
    def test_disconnect_unsets_the_socket(self, socket):
        client = Client("localhost", port=0)

        client.connect()

        assert client.socket is not None

        client.disconnect()

        assert client.socket is None

        socket.socket.return_value.close.assert_called_once_with()

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
