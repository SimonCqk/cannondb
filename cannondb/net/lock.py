"""
Distributed lock deployed on the server, because multi-client may submit
read/write requests concurrently, so we have to take steps to avoid contention.
"""
import redis
import uuid
import time
from cannondb.net.utils import redis_manual_launcher


class DistributedLock:
    def __init__(self):
        redis_manual_launcher()

    @staticmethod
    def acquire(conn: redis.Connection(), lock_name: str, timeout=10):
        identifier = str(uuid.uuid4())  # generate a random UUID as mutex
        end = time.time() + timeout
        while time.time() < end:
            if conn.setnx('lock:' + lock_name, identifier):
                return identifier
        # timeout and acquire failed
        return False

    @staticmethod
    def release(conn: redis.Redis, lock_name: str, identifier):
        lock_name = 'lock:' + lock_name
        pipe = conn.pipeline(True)
        while True:
            try:
                pipe.watch(lock_name)
                # check if current process still holds the lock
                if pipe.get(lock_name) == identifier:
                    # release lock
                    pipe.multi()
                    pipe.delete(lock_name)
                    pipe.execute()
                    return True
                pipe.unwatch()
                break
            except redis.WatchError:
                pass
        # current process has lost the lock
        return False
