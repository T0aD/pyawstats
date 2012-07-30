#! /usr/bin/python3.1

# This module merely creates a lock file in order to lock the execution of a python script
# (avoids running twice the same script at the same time..)
import os.path
import sys

"""
=========================== Usage:
import lock

with lock.Lock(__file__):
  main program

=========================== Or:
from lock import Lock

with Lock():
  main program
"""

class Lock():
    path = '/var/tmp'

    # Just generate the full sexy path to the lockfile
    def __init__(self, name = False, path = False):
        # Leet hack if no name was specified:
        if name is False:
            name = sys.argv[0]
            # Seems overkill now that argv[0] seems to be OK (why did I want that anyway?)
#            name = sys._getframe(1).f_code.co_filename 
            name = os.path.basename(name)
        if not path is False:
            self.path = path
        if name.endswith('.py'):
            name = name[:len(name)-3]
        self.lockfile = os.path.realpath(os.path.join(self.path, name + '.lock'))

    # Create the lockfile and writes the PID in it
    def __enter__(self):
        try:
            fd = os.open(self.lockfile, os.O_CREAT | os.O_EXCL | os.O_RDWR)
        except:
            fd = open(self.lockfile)
            pid = fd.read()
            fd.close()
            # Try to see if a process is actually running with this PID:
            # (Linux)
            if os.path.exists('/proc/%s/cmdline' % pid):
                fd = open('/proc/%s/cmdline' % pid)
                prog = fd.read()
                fd.close()
                running = 'Program still running: %s' % prog.replace('\0', ' ')
            else:
                running = 'No process running'
            print("lockfile %s exists with PID %s (%s)" % (self.lockfile, pid, running))
            exit(1)
        os.write(fd, bytes(str(os.getpid()), 'ascii'))
        os.close(fd)

    # Only remove the lockfile when no exception was raised
    def __exit__(self, etype, evalue, traceback):
        if etype is None:
            os.unlink(self.lockfile)

