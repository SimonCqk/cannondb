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

    def acquire(self, conn: redis.Redis, acquire_timeout=2.0, lock_timeout=5.0) -> bool:
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
    # in 32 bit platform, redis-int type ranges up to 2^31-1, so for insurance we'd recycle
    # the counter.
    COUNTER_MOD = pow(2, 31)

    def __init__(self, name, sem_val: int = None):
        self._name = 'semaphore:' + name
        self._set_name = self._name + ':owner'  # owners of this semaphore
        self._counter_name = self._name + ':counter'  # counter of this semaphore
        self._id = str(uuid.uuid4())  # generate a 128 bits random UUID as identifier
        self._lock = DistributedLock(self._name)
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
    This impl of semaphore-acquisition ignore the fact that the timestamp of different process may 
    differ, but it runs fast and simple enough. 
    """

    def fast_acquire(self, conn: redis.Redis, timeout=5.0):
        now = time.time()
        pipe = conn.pipeline(True)
        # remove all expired semaphore
        pipe.zremrangebyscore(self._name, '-inf', now - timeout)
        # attempt to acquire semaphore
        pipe.zadd(self._name, self._id, now)
        # check the ranking
        pipe.zrank(self._name, self._id)
        if pipe.execute()[-1] < self.SEM_VAL_LIMIT:
            return True
        # client failed to acquire, undo ops
        conn.zrem(self._name, self._id)
        return False

    """
    These implementations of semaphore-acquisition has considered the time-sync of different hosts, 
    so forget about unfair issues.
    - acquire_with_lock() avoid contentions and make acquisition absolutely act right, while
      acquire_without_lock() may cause racing under some situation, I do advise use acquire_with_lock()
      as it's necessary though little performance lost.
    """

    def acquire_with_lock(self, conn: redis.Redis, timeout=5.0) -> bool:
        lock_res = self._lock.acquire(conn, acquire_timeout=0.01)
        if lock_res:
            try:
                return self.acquire_without_lock(conn, timeout)
            finally:
                self._lock.release(conn)
        return False

    def acquire_without_lock(self, conn: redis.Redis, timeout=5.0) -> bool:
        now = time.time()
        pipe = conn.pipeline(True)
        # remove all expired semaphore
        pipe.zremrangebyscore(self._name, '-inf', now - timeout)
        pipe.zinterstore(self._set_name, [self._set_name, self._name])

        pipe.incr(self._counter_name)
        # update counter value and get it
        counter = pipe.execute()[-1] % self.COUNTER_MOD
        # attempt to acquire semaphore
        pipe.zadd(self._name, self._id, now)
        pipe.zadd(self._set_name, self._id, counter)
        # check the ranking
        pipe.zrank(self._set_name, self._id)
        if pipe.execute()[-1] < self.SEM_VAL_LIMIT:
            return True
        # client failed to acquire, undo ops
        pipe.zrem(self._name, self._id)
        pipe.zrem(self._set_name, self._id)
        pipe.execute()
        return False

    def release(self, conn: redis.Redis) -> bool:
        """
        :return: True: release semaphore properly
                 False: semaphore has been removed because of expired
        """
        pipe = conn.pipeline(True)
        pipe.zrem(self._name, self._id)
        pipe.zrem(self._set_name, self._id)
        return pipe.execute()[0]

    def refresh(self, conn: redis.Redis) -> bool:
        """refresh semaphore so as to reset timeout"""
        # if `ZADD` return 1(new timestamp added successfully),  which means the client
        # has lost this semaphore, else client still holds it.
        if conn.zadd(self._name, self._id, time.time()):
            self.release(conn)
            return False
        return True
