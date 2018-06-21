import os
import subprocess
import time


class RedisNotLaunchedError(Exception):
    pass


def redis_manual_launcher():
    """
    If redis-server has launched, just return, else run launch cmd
    manually in shell.
    """
    if not has_redis_launched():
        # os.system() will block the whole program, so avoid to use it.
        subprocess.Popen('redis-server', shell=True, stdout=subprocess.PIPE)
        '''
        guarantee the server has started, since sub process module executes cmd asynchronously,
        I'm not sure whether `sleep` is necessary or not.
        '''
        time.sleep(0.01)
        if not has_redis_launched():
            raise RedisNotLaunchedError('redis server cannot launch properly, please check the configuration')


def has_redis_launched() -> bool:
    """
    Check if redis-server has launched on this host
    """
    # check the owner of port 6379
    output = os.popen('lsof -i:6379').read()
    if len(output) == 0:
        return False
    '''
    Output should be shown like this:
    [COMMAND PID USER FD TYPE DEVICE SIZE/OFF NODE NAME]
    '''
    rows = output.split('\n')[1:]
    for row in rows:
        row = row.split()
        if 'redis-ser' in row:
            return True
    return False
