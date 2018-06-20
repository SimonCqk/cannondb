"""
Distributed lock deployed on the server, because multi-client may submit
read/write requests concurrently, so we have to take steps to avoid contention.
"""
import redis
import uuid
import time
import math
from cannondb.net.utils import redis_manual_launcher


class DistributedLock:
    def __init__(self):
        redis_manual_launcher()

    @staticmethod
    def acquire(conn: redis.Connection(), lock_name: str, acquire_timeout=2, lock_timeout=5):
        identifier = str(uuid.uuid4())  # generate a 128 bits random UUID as mutex
        end = time.time() + acquire_timeout
        lock_timeout = int(math.ceil(lock_timeout))
        while time.time() < end:
            """
            'ex': set expire time
            'nx': set only if key not exists
            """
            if conn.set(lock_name, identifier, ex=lock_timeout, nx=True):
                return identifier
            elif not conn.ttl(lock_name):
                # check the expire time and update it if needed
                conn.expire(lock_name, lock_timeout)
            time.sleep(0.001)  # wait for 1ms
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
