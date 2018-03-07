"""
  This file include storage wrappers to expand the functionality of storage.
- log_wrapper: Use logs to record all pivotal operations.
"""
import datetime
import logging
import os


def log_wrapper(cls, methods_to_log: tuple = ('add', 'decrease'), config='local'):
    logger = logging.getLogger(cls.__class__.__name__)
    if config == 'socket':
        pass
    else:
        if not os.path.exists('log.log'):
            os.open('log.log', os.O_RDWR | os.O_CREAT)
        logger.addHandler(logging.FileHandler('log.log', mode='w+'))

    orig_getattr = cls.__class__.__getattribute__

    def _wrapper(self, name):
        if name in methods_to_log:
            logger.warning('{cls} {method} at {time}'.format(cls=cls.__class__.__name__, method=name,
                                                             time=datetime.datetime.now().isoformat()))
        return orig_getattr(self, name)

    cls.__class__.__getattribute__ = _wrapper
    return cls
