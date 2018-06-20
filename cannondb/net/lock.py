import math
import time
import uuid

import redis

from cannondb.constants import DEFAULT_SEM_VAL
from cannondb.net.utils import redis_manual_launcher


class DistributedLock:
    """
    Mainly deployed on the server. Multi-client from different hosts may submit read/write
    requests concurrently, so we have to take steps to avoid contention.
    """

    def __init__(self, name: str):
        self._name = 'lock:' + name
        self._id = str(uuid.uuid4())  # generate a 128 bits random UUID as identifier
        redis_manual_launcher()

    def acquire(self, conn: redis.Redis, acquire_timeout=2, lock_timeout=5) -> bool:
        end = time.time() + acquire_timeout
        lock_timeout = int(math.ceil(lock_timeout))
        while time.time() < end:
            """
            'ex': set expire time
            'nx': set only if key not exists
            """
            if conn.set(self._name, self._id, ex=lock_timeout, nx=True):
                return True
            elif not conn.ttl(self._name):
                # check the expire time and update it if needed
                conn.expire(self._name, lock_timeout)
            time.sleep(0.001)  # wait for 1ms
        # timeout and acquire failed
        return False

    def release(self, conn: redis.Redis) -> bool:
        pipe = conn.pipeline(True)
        while True:
            try:
                pipe.watch(self._name)
                # check if current process still holds the lock
                if pipe.get(self._name) == self._id:
                    # release lock
                    pipe.multi()
                    pipe.delete(self._name)
                    pipe.execute()
                    return True
                pipe.unwatch()
                break
            except redis.WatchError:
                pass
        # current process has lost the lock
        return False


class DistributedSemaphore:
    """
    Unlike Distributed Lock, it provides scalability with the amount of hosts-concurrent-request,
    """
    SEM_VAL_LIMIT = DEFAULT_SEM_VAL

    def __init__(self, name, sem_val: int = None):
        self._name = name
        self._set_name = name + ':owner'  # owners of this semaphore
        self._counter_name = name + ':counter'  # counter of this semaphore
        self._id = str(uuid.uuid4())  # generate a 128 bits random UUID as identifier
        if sem_val:
            self.SEM_VAL_LIMIT = sem_val
        redis_manual_launcher()

    @property
    def sem_value(self):
        return self.SEM_VAL_LIMIT

    @sem_value.setter
    def sem_value(self, val):
        raise RuntimeError('The value of distributed semaphore is read-only')

    """
    This impl of semaphore has considered the time-sync of different hosts, so forget about unfair issues.
    """

    def acquire(self, conn: redis.Redis, timeout=5) -> bool:
        now = time.time()
        pipe = conn.pipeline(True)
        # remove all expired semaphore
        pipe.zremrangebyscore(self._name, '-inf', now - timeout)
        pipe.zinterstore(self._set_name, [self._set_name, self._name])

        pipe.zincrby(self._counter_name)
        # update counter value and get it
        counter = pipe.execute()[-1]
        # attempt to acquire semaphore
        pipe.zadd(self._name, self._id, now)
        pipe.zadd(self._set_name, self._id, counter)
        # check the ranking
        pipe.zrank(self._set_name, self._id)
        if pipe.execute()[-1] < self.SEM_VAL_LIMIT:
            return True
        # TODO: complete

    def release(self):
        pass
