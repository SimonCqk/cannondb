from cannondb.wrapper import log_wrapper


class TestWrapper(object):
    def __init__(self, a):
        self._a = a

    def add(self, a):
        self._a += a

    def decrease(self, a):
        self._a -= a

    def reset(self, a):
        self._a = a


if __name__ == '__main__':
    test = TestWrapper(0)
    mtl = ('add', 'decrease')
    test = log_wrapper(test, mtl)
    test.add(1)
    test.add(2)
    test.decrease(3)
    test.reset(0)
    # check the log file
