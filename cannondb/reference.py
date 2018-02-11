from cannondb.btree import BNode


class ValueRef(object):
    def __init__(self, referent=None, address=0):
        self._referent = referent
        self._address = address

    @staticmethod
    def dump(referent):
        return referent.encode('utf-8')

    @staticmethod
    def load(string):
        return string.decode('utf-8')

    def read_data(self, storage):
        if self._referent is None and self._address:
            self._referent = self.load(
                storage.read(self._address))
        return self._referent

    def write_data(self, storage):
        if self._referent is not None:
            self.prepare_to_store(storage)
            self._address = storage.write(
                self.dump(
                    self._referent), self._address)

    @property
    def address(self):
        return self._address

    @address.setter
    def address(self, value):
        raise RuntimeError("Can't set address over referent.")


class LeafRef(ValueRef):

    def prepare_to_store(self, storage):
        if self._referent:
            self._referent.store_refs(storage)

    @property
    def length(self):
        pass

    @staticmethod
    def dump(referent):
        return pickle.dumps({
            'tree': referent.tree,
            'keys': referent.contents,
            'values': referent.data,
        })

    @staticmethod
    def load(string):
        d = pickle.loads(string)
        return BNode(
            tree=d['tree'],
            keys=d['keys'],
            children=d['values']
        )
