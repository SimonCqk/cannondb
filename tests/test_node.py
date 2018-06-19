from cannondb.btree import BTree
from cannondb.node import BNode, KeyValPair
from cannondb.constants import TreeConf
from tests.util import refine_test_file

file_name = refine_test_file('test_node')
test_tree = BTree(file_name, 3, 1024, 8, 12, cache_size=0)
test_tree_conf = TreeConf(3, 1024, 8, 12)
test_contents = [
    KeyValPair(test_tree_conf, '1', 1),
    KeyValPair(test_tree_conf, '2', 2),
    KeyValPair(test_tree_conf, '3', 3),
    KeyValPair(test_tree_conf, '4', 4),
    KeyValPair(test_tree_conf, '5', 5)
]
test_children = [0, 1, 2, 3, 4, 6]

"""
Actually there is no so much cases to independently test on BNode,
the insert/remove/grow/shrink operations should reflect on the ops 
in BTree.
"""


def test_load_dump():
    node = BNode(test_tree, test_tree_conf, contents=test_contents, children=test_children)
    dumped = node.dump()
    loaded_node = BNode(test_tree, test_tree_conf, data=dumped)
    print(repr(loaded_node))


def test_split():
    node = BNode(test_tree, test_tree_conf, contents=test_contents, children=test_children)
    sib, mid = node.split()
    assert len(sib.contents) == len(node.contents)
    assert len(sib.children) == len(node.children)
    print('repr sib:', repr(sib))
    print('repr mid', repr(mid))


def test_ops_in_dump():
    """
    test all internal operations in dumped data
    """
    for_op = KeyValPair(test_tree_conf, 'op', -1)
    node = BNode(test_tree, test_tree_conf, contents=test_contents, children=[0, 1, 2, 3, 4, 6])
    node.dump()
    node.insert_content_in_dump(0, for_op)
    node.insert_child_in_dump(0, 9)
    assert len(BNode(test_tree, test_tree_conf, data=node.dump()).contents) == len(test_contents) + 1
    assert len(BNode(test_tree, test_tree_conf, data=node.dump()).children) == len(test_children) + 1
    node.pop_content_in_dump(0)
    node.pop_child_in_dump(0)
    assert len(BNode(test_tree, test_tree_conf, data=node.dump()).contents) == len(test_contents)
    assert len(BNode(test_tree, test_tree_conf, data=node.dump()).children) == len(test_children)
    node.update_child_in_dump(0, 0)
    node.update_content_in_dump(0, for_op)


test_tree.commit()
test_tree.close()
