import pickle

from cannondb.logical import ValueRef
from cannondb.tree import _BPlusLeaf


class LeafRef(ValueRef):

    def prepare_to_store(self, storage):
        if self._referent:
            self._referent.store_refs(storage)

    @property
    def length(self):
        pass

    @staticmethod
    def referent_to_string(referent):
        return pickle.dumps({
            'tree': referent.tree,
            'contents': referent.contents,
            'data': referent.data,
        })

    @staticmethod
    def string_to_referent(string):
        d = pickle.loads(string)
        return _BPlusLeaf(
            tree=d['tree'],
            contents=d['contents'],
            data=d['data']
        )
