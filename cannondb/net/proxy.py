import socket
from abc import ABCMeta, abstractmethod

from cannondb.net.utils import AESCipher, AES_KEY_LEN
from cannondb.serializer import StrSerializer


class AbstractProxy(metaclass=ABCMeta):
    BUFFER_SIZE = 2048
    KEY_BUFFER_SIZE = AES_KEY_LEN

    @abstractmethod
    def send(self, msg: bytes):
        pass

    @abstractmethod
    def receive_from(self):
        pass


class ClientProxy(AbstractProxy):

    def __init__(self):
        self._server_key = None
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    def send(self, msg: bytes):
        pass

    def receive_from(self):
        pass

    def get_key_from_server(self):
        """
        Receive key sent from server socket. It should be done instantly when client connects
        to the server successfully, server send its private key as response.
        """
        key = self._sock.recv(self.KEY_BUFFER_SIZE)
        assert len(key) == self.KEY_BUFFER_SIZE
        # deserialize key in bytes to string
        self._server_key = StrSerializer.deserialize(key)


class ServerProxy(AbstractProxy):

    def __init__(self):
        self._aes = AESCipher()
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    def send(self, msg: bytes):
        pass

    def receive_from(self):
        pass

    def _respond_key(self, cli: ClientProxy):
        self._aes.transmit_key_to_client(cli)
