from collections import namedtuple

__all__ = ['PAGE_ADDRESS_LIMIT', 'PAGE_LENGTH_LIMIT', 'PAGE_ADDRESS_FORMAT', 'ENDIAN', 'KEY_LENGTH_LIMIT',
           'KEY_LENGTH_FORMAT', 'VALUE_LENGTH_FORMAT', 'VALUE_LENGTH_LIMIT', 'NODE_TYPE_LENGTH_LIMIT',
           'SERIALIZER_TYPE_LENGTH_LIMIT', 'NODE_CONTENTS_LENGTH_LIMIT', 'INT_FORMAT', 'FLOAT_FORMAT',
           'FRAME_TYPE_LENGTH_LIMIT', 'DEFAULT_LOGGER_NAME', 'METHODS_TO_LOG', 'TreeConf', 'DEFAULT_CHECKPOINT_SECONDS']

# network (= big-endian)
ENDIAN = 'big'

# bytes for indexing page : unsigned long long , big-endian, size=4
PAGE_ADDRESS_FORMAT = '!I'
PAGE_ADDRESS_LIMIT = 4

# bytes for storing length of each page
PAGE_LENGTH_LIMIT = 3

# bytes for storing _key and _value: unsigned short, big-endian, size=2
# limit length of _key under 64KB. [64KB = 2^16bit]
# length of _value should be bigger because `float` takes 4 bytes, `float`
# should be supported but`double` takes too much(8 bytes) so we ignore it.
KEY_LENGTH_FORMAT = '!H'
VALUE_LENGTH_FORMAT = '!I'
KEY_LENGTH_LIMIT = 2
VALUE_LENGTH_LIMIT = 4

# bytes for storing node type
NODE_TYPE_LENGTH_LIMIT = 1

# bytes for storing node contents size
NODE_CONTENTS_LENGTH_LIMIT = 2

# bytes for storing serializer type
SERIALIZER_TYPE_LENGTH_LIMIT = 1

# bytes for storing per frame used in WAL module
FRAME_TYPE_LENGTH_LIMIT = 1

INT_FORMAT = '!l'
FLOAT_FORMAT = '!f'

# default seconds to do checkpoint
DEFAULT_CHECKPOINT_SECONDS = 120

DEFAULT_LOGGER_NAME = 'Logger'

METHODS_TO_LOG = (
    'remove',
    'get',
    'checkpoint',
    'commit',
    'set_auto_commit',
    'close'
)

TreeConf = namedtuple('TreeConf', [
    'order',  # order of B tree
    'page_size',  # Size of a page within the tree in bytes
    'key_size',  # Maximum size of a _key in bytes
    'value_size',  # Maximum size of a _value in bytes
])
