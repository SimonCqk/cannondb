"""
Distributed lock deployed on the server, because multi-client may submit
read/write requests concurrently, so we have to take steps to avoid contention.
"""


class DistributedLock:
    pass
