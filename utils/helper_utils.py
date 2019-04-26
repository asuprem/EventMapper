import time
from datetime import datetime
import sys


# Checks if two dictionaries are equal
# TODO optimize this
def dict_equal(d1, d2):
    """ return True if all keys and values are the same """
    flag1= True
    flag2= True
    for key in d1:
        if not (key in d2 and d1[key] == d2[key]):
            flag1 = False
    for key in d2:
        if not (key in d1 and d1[key] == d2[key]):
            flag2 = False
    return flag1 and flag2


#setu up PID for recurrence checks
def setup_pid(pid_name):
    import os, sys
    #pid_name will be application name -- '/path/app.py'
    pid = str(os.getpid())
    pidFile = './logfiles/' + pid_name + '.pid'

    if os.path.isfile(pidFile):
        print("pidfile already exists. exiting")
        sys.exit()
    open(pidFile,'w').write(pid)

def readable_time():
    return datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')

def std_flush(*args,**kwargs):
    print(" ".join(map(str,args)))
    sys.stdout.flush()