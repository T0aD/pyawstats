#! /usr/bin/python3.1

# This module merely creates a lock file in order to lock the execution of a python script
# (avoids running twice the same script at the same time..)
import os.path

"""
Usage:
import lock

with lock.lock(__file__):
  main program
"""

class lock():
    path = '/var/tmp'

    # Just generate the full sexy path to the lockfile
    def __init__(self, name):
        self.lockfile = os.path.realpath(os.path.join(self.path, name.replace('.py', '.lock')))

    # Create the lockfile and writes the PID in it
    def __enter__(self):

        try:
            fd = os.open(self.lockfile, os.O_CREAT | os.O_EXCL | os.O_RDWR)
        except:
            fd = open(self.lockfile)
            pid = fd.read()
            fd.close()
            print("lockfile %s exists with PID %s" % (self.lockfile, pid))
            exit(1)
        # I .. I dont know how to do that....   :_(
#        os.write(fd, os.getpid())
        os.close(fd)
        fd = open(self.lockfile, 'w')
        fd.write(str(os.getpid()))
        fd.close()

    # Only remove the lockfile when no exception was raised
    def __exit__(self, etype, evalue, traceback):
        if etype is None:
            os.unlink(self.lockfile)

