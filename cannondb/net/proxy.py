import queue
import socket
import logging
import threading
from abc import ABCMeta, abstractmethod

from cannondb.net.utils import AESCipher, AES_KEY_LEN
from cannondb.serializer import StrSerializer
from cannondb.constants import DEFAULT_LOGGER_NAME, SERVER_PORT

logger = logging.getLogger(DEFAULT_LOGGER_NAME)


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

    def connect_to_server(self, host, port):
        self._sock.connect((host, port))

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
    MAX_LISTEN_COUNT = 1024

    def __init__(self):
        self._aes = AESCipher()
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._sock.bind(('127.0.0.1', SERVER_PORT))
        self._sock.listen(self.MAX_LISTEN_COUNT)
        self._cli_queue = queue.deque()
        self._closed = False
        # background listening thread
        self._listen_th = threading.Thread(target=self._listen).start()

    def _listen(self):
        # listening for new coming clients.
        while not self._closed:
            cli, address = self._sock.accept()
            logger.info("accept client with address: {0}".format(address))
            self._cli_queue.append(cli)
            # once connection build, server sent its pub-key as response
            self._respond_key(cli)

    def send(self, msg: bytes):
        pass

    def receive_from(self):
        pass

    def _respond_key(self, cli: ClientProxy):
        self._aes.transmit_key_to_client(cli)
