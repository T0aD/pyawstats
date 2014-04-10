#! /usr/bin/env python3

import io
import sys
import time
import timer
import apparser
import mmap
import os
import concurrent.futures
import resource

try:
    f = sys.argv[1]
    w = int(sys.argv[2])
except:
    w = 4

st = os.stat(f)
chunksize = int(st.st_size / w)
print('%s %d MB chunk: %d MB' % (f, st.st_size / (1024*1024), 
                                 chunksize / (1024*1024)))
executor = concurrent.futures.ProcessPoolExecutor(max_workers=w)
rt = timer.timer()

fd = open(f)
mm = mmap.mmap(fd.fileno(), 0, prot=mmap.PROT_READ)
fd.close()

def getmem(label=False):
    if label is False:
        label = ''
    mem = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    print('%smem: %d KB' % (label, mem))

def iterate_log(filename, start, end):
    mapping = timer.timer()

    fd = open(filename)
    mm = mmap.mmap(fd.fileno(), 0, prot=mmap.PROT_READ)
    eob = end
    end = -1

    while True:
        if start == eob:
            break
        end = mm.find(b'\n', start)
        if end == -1:
            break
        line = mm[start:end]
#        print('[%10d:%10d:%10d]' % (start, end, end - start))
        yield line.decode()
#        yield line
        start = end + 1
    fd.close()

offsets = []
# Divide blocks of the file: we need to figure out where to cut precisly
# (looking for \n)
offset = 0
for i in range(w):
#    offset = i * chunksize
    end = ((i + 1) * chunksize) - 1
    mm.seek(end)
    off = mm.find(b'\n') + 1
#    print('seek: %10d found: %10d correction: %10d' % (end, off, off-end))
    offsets.append({'filename': f, 'start': offset, 'end': off})
    offset = off
fd.close()

rt.stop('mmaped %s' % f, 0)
#exit()

class uniq:
    def __init__(self):
        self.cache = {}
        self.id = 1
    def get(self, name, container):
        if not name in self.cache:
            print('+ vhost %s (%d) (container: %d)' % 
                  (name, self.id, container))
            self.cache[name] = self.id
            self.id += 1
        return self.cache[name]

u = uniq()

# This is the worker
def consumer(index, filename, start, end, u):
    parser = apparser.apparser()
    t = timer.timer()
#    print('started job %d' % index)
    i = 0
    t2 = timer.timer()
#    map(parser.proto, iterate_log(filename, start, end))
#    t.stop('job %d' % index, i)
#    return (index, i)

    for l in iterate_log(filename, start, end):
#        if (i % 100001) == 0 and not i == 0:
#            t2.stop('[%d] parsed %10d lines' % (index, i), i)
        i += 1
#        r = apparser._parse2(l)
        r = parser.proto(l)
#        break
#        print(parser)
#        u.get(parser.vhost, index)
    t.stop('job %d' % index, i)
    return (index, i)

# Giving work to those lazy cores:
parsingTimer = timer.timer()
futures = []
worker = 0
for offset in offsets:
    f, s, e = offset['filename'], offset['start'], offset['end']
    r = executor.submit(consumer, worker, f, s, e, u)
    worker += 1
    futures.append(r)
total = 0
for future in concurrent.futures.as_completed(futures):
    jobID, lines = future.result()
    total += lines
parsingTimer.stop('%d jobs' % w, total)
