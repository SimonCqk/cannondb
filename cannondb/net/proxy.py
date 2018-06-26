from collections import namedtuple

import rsa

RSATuple = namedtuple('RSATuple', ['public_key', 'private_key'])


class ClientProxy:
    pass


class ServerProxy:

    def __init__(self):
        self._rsa = RSATuple(*rsa.newkeys(256))
