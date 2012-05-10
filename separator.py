#! /usr/bin/python3.1

##
# 1/ Fetch a queue of logfiles in a sqlite database
# 2/ Read each logfile and create new logfiles according to dates 
#       (-> 2011/01/01.tmp for log line dated 1 Jan 2011)
#       if exists: name it something else ! like 01-1.tmp 
# 3/ When one logfile is done, move it to 2011/01/01.log
# 4/ Mark it as done in the sqlite queue
# 5/ Carry on

import sys
import os.path
import os
import io
import sqlite3

def die(msg):
    sys.stderr.write(msg + '\n')
    exit()

#if not len(sys.argv) == 2:
#    die('Syntax: %s <apache logfile>' % sys.argv[0])
#if not os.path.exists(sys.argv[1]):
#    die('file %s does not exist' % sys.argv[1])

import logging
logging.basicConfig(filename='error.log', level=logging.DEBUG,
                    format='%(asctime)s %(levelname)7s: %(message)s')

import time
class timer:
    def __init__(self, lines = 0, size = 0):
        self.time, self.lines, self.size = (time.time(), lines, size)
    def stop(self, msg, lines, size):
        delta = time.time() - self.time
        deltalines = lines - self.lines
#        deltasize = size - self.size
#        info = '%d l/s %0.2f MB/s' % (deltalines / delta, (deltasize/(1024*1024)) / delta)
        info = '%d l/s' % (deltalines / delta,)
        if delta < 0.5:
            #            print('*ding!* %s took %0.2f ms %s' % (msg, delta * 1000, info))
            print('%s took %0.2f ms %s' % (msg, delta * 1000, info))
        else:
            #            print('*ding!* %s took %0.2f s %s' % (msg, delta, info))
            print('%s took %0.2f s %s' % (msg, delta, info))

class newlog:
    def __init__(self, fd, path, day, suffix, filename):
        self.fd, self.path, self.day, self.suffix, self.filename = (fd, path, day, suffix, filename)

class separator:
    def __init__(self, path):
        self.path = path # where to store the new logs
        self.fd = {} # storage for file descriptors
    # Write the log line in the good file
    def write(self, year, month, day):
        id = year + month + day
        if id not in self.fd:
            self.fd[id] = self.createfile(year, month, day)
        return self.fd[id].fd

    def createfile(self, year, month, day):
        path = os.path.join(self.path, year, month)
        self._checkpath(path)
        suffix = None
        if os.path.exists(os.path.join(path, '%s.log' % day)):
            suffix = self._getsuffix(path, day)
        if suffix is None:
            filename = '%s/%s.tmp' % (path, day)
        else:
            filename = '%s/%s-%d.tmp' % (path, day, suffix)
        print('creating file', filename)
        fd = open(filename, 'w')
        # Create a new structure to store the data we'll need later
        return newlog(fd, path, day, suffix, filename)

    # Returns the first available suffix
    def _getsuffix(self, path, name):
        i = 1
        while os.path.exists(os.path.join(path, '%s-%d.log' % (name, i))):
            i += 1
        return i

    def _checkpath(self, path):
        if not os.path.exists(path):
            newpath = os.path.dirname(path)
            self._checkpath(newpath)
            os.mkdir(path)

    # Will move tmp to log
    def finish(self):
        for id in list(self.fd.keys()):
            l = self.fd[id]
            l.fd.close()
            if l.suffix is None:
                filename = '%s.log' % (l.day)
            else:
                filename = '%s-%d.log' % (l.day, l.suffix)
            logfile = os.path.join(l.path, filename)
            print('renaming %s to %s' % (l.filename, logfile))
            if os.path.exists(logfile):
                raise Exception('logfile already present: %s' % logfile)
#            print('removing', l.filename)
#            os.unlink(l.filename)
            os.rename(l.filename, logfile)
            del self.fd[id]

    def abort(self):
        for opened in self.fd:
            self.fd[opened].fd.close()
        self.fd = {}
        

# Parsing function
# Kudos go to flox @freenode #python-fr
months = {'Jan': '01', 'Feb': '02', 'Mar': '03', 'Apr': '04', 'May': '05', 'Jun':'06',
          'Jul':'07', 'Aug':'08', 'Sep':'09', 'Oct': '10', 'Nov':'11', 'Dec':'12'}
def parse(line):
    idx = line.find('[')
    return line[idx + 8:idx + 12], months[line[idx + 4:idx + 7]], line[idx + 1:idx + 3]
(slice_day, slice_month, slice_year) = slice(2), slice(3, 6), slice(7, 11)
def parse2(line):
    line, line, line = line.partition(' [')
    return line[slice_year], months[line[slice_month]], line[slice_day]


import re
miniPattern = '^[^ ]+ [^ ]+ [^ ]+ [^ ]+ \[(.+?)\]'
pat = re.compile(miniPattern)

def parseFile(filename):
    fileTimer = timer()
    print('=' * 30, filename)
    fd = open(filename, 'r')

    t = timer()
    i = 0
    length = 0
    for line in fd:
        i += 1

        # Eats 20,000 lines per second:
        #    length += len(line) 
        try:
            year, month, day = parse2(line)
        except:
            print('Couldnt parse line (1st pass):', line)
            # Trying a more complete regex...:

            m = pat.match(line)
            if not m is None:
                try:
                    d = m.group(1)
                    year, month, day = d[7:11], months[d[3:6]], d[0:2]
                    print('second pass worked!', year, month, day)
                except:
                    print('Second pass failed... skipping to next file')
                    logging.error('second pass failed...')
                    separator.abort()
                    return False
            else:
                print('=' * 20, 'Skipping to the next file in list....')
                logging.error('cannot parse %s:%d %s' % (filename, i, line))
                separator.abort()
                return False
        fd = separator.write(year, month, day)
        fd.write(line)

        if i % 100001 == 0:
            t.stop('parsed %d lines' % i, i, length)
            t = timer(i, length)

    separator.finish()
    db.execute('UPDATE queue SET done = 1 WHERE name = ?', (filename, ))
    db.commit()
    fileTimer.stop('parsed %s' % filename, i, length)


i = 0
length = 0
mainTimer = timer() # timing the whole program
separator = separator('./logs/final/')

# List all the files to be worked on..:
db = sqlite3.connect('queue.sqlite')
if len(sys.argv) == 2:
    query = 'SELECT name FROM queue WHERE done = 0 AND name = ? ORDER BY id ASC'
    r = db.execute(query, (sys.argv[1],))
else:
    query = 'SELECT name FROM queue WHERE done = 0 ORDER BY id ASC'
    r = db.execute(query)

for f in r.fetchall():
    f = f[0]
    parseFile(f)
mainTimer.stop('end of script', i, length)

