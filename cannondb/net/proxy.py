from abc import ABCMeta

from cannondb.net.utils import AESCipher


class AbstractProxy(metaclass=ABCMeta):

    def send(self, msg: bytes):
        pass

    def receive_from(self):
        pass


class ClientProxy(AbstractProxy):

    def __init__(self):
        self._server_key = None

    def send(self, msg: bytes):
        pass

    def receive_from(self):
        pass

    def get_key_from_cipher(self, key: str):
        self._server_key = key


class ServerProxy(AbstractProxy):

    def __init__(self):
        self._aes = AESCipher()
