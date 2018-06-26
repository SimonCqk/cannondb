"""
Parsers for commands transported through network.
Cannon db supports three basic methods called by clients and responded by server.
- GET: query a key and return its corresponding value. If key doesn't exist, return
       a special code which stands for error.
- INSERT: post a key-value pair and store it into server host.
- REMOVE: remove a key-value pair in server host by its key specified by client.
          If key doesn't exist, do nothing.
the format works like this:
<method> <key> [<value>]
and each transmission will be encrypted for security.
"""


class CmdParser:

    def __init__(self):
        pass
