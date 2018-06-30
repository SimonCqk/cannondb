import os
import random
import subprocess
import time

from Crypto.Cipher import AES


class RedisNotLaunchedError(Exception):
    pass


class AESKeyNotPermit(PermissionError):
    pass


def redis_manual_launcher():
    """
    If redis-server has launched, just return, else run launch cmd
    manually in shell.
    """
    if not has_redis_launched():
        # os.system() will block the whole program, so avoid to use it.
        subprocess.Popen('redis-server', shell=True, stdout=subprocess.PIPE)
        '''
        guarantee the server has started, since sub process module executes cmd asynchronously,
        I'm not sure whether `sleep` is necessary or not.
        '''
        time.sleep(0.01)
        if not has_redis_launched():
            raise RedisNotLaunchedError('redis server cannot launch properly, please check the configuration')


def has_redis_launched() -> bool:
    """
    Check if redis-server has launched on this host
    """
    # check the owner of port 6379
    output = os.popen('lsof -i:6379').read()
    if len(output) == 0:
        return False
    '''
    Output should be shown like this:
    [COMMAND PID USER FD TYPE DEVICE SIZE/OFF NODE NAME]
    '''
    rows = output.split('\n')[1:]
    for row in rows:
        row = row.split()
        if 'redis-ser' in row:
            return True
    return False


AES_KEY_LEN = 16  # key length of AES must be 16/24/32, corresponding to AES-125/AES-192/AES-256.


class AESCipher:
    """
    Cipher for AES encryption and decryption, we add an extra protection while transmitting through network.
    We choose AES but not DES/RSA...etc because of its **good performance** and security.
    """
    __slots__ = ['_key', 'cipher']

    def __init__(self, key=None):
        assert len(key) <= AES_KEY_LEN, 'key length no more than {len} bytes'.format(len=AES_KEY_LEN)
        if key and len(key) < AES_KEY_LEN:
            self._key = self.__aes_padding(key)
        elif not key:
            self._key = self._random_key_generator()
        else:
            self._key = key
        self.cipher = AES.new(self._key, AES.MODE_CBC, self._key)

    def encrypt(self, plain_text):
        return self.cipher.encrypt(self.__aes_padding(plain_text))

    def decrypt(self, encoded):
        return self.__aes_un_padding(self.cipher.decrypt(encoded))

    def transmit_key_to_client(self, cli):
        """
        Since the most important part of AES is the key, and we shall never just exposed key to
        a casual function invoker, so we'd guarantee the key transmit to the exact client or its
        proxy.
        """
        from cannondb.net.proxy import ClientProxy
        if not isinstance(cli, ClientProxy):
            raise AESKeyNotPermit('key should only be transmitted to client or its proxy.')

        cli.send(self._key.encode(encoding='utf-8'))

    @staticmethod
    def _random_key_generator() -> str:
        """Generate a stable length(set by AES_KEY_LEN) key string"""
        chars = 'AaBbCcDdEeFfGgHhIiJjKkLlMmNnOoPpQqRrSsTtUuVvWwXxYyZz0123456789'
        chars_len = len(chars)
        s = ''
        for i in range(AES_KEY_LEN):
            s += chars[random.randrange(0, chars_len - 1)]
        return s

    @staticmethod
    def __aes_padding(s: str) -> str:
        """Padding for s if length is not enough"""
        size = len(s)
        add = AES_KEY_LEN - (size % AES_KEY_LEN)
        byte = chr(add)
        return s + add * byte

    @staticmethod
    def __aes_un_padding(s: str) -> str:
        """Remove extra bytes padded before"""
        byte = s[-1]
        add = ord(byte)
        return s[:-add]
