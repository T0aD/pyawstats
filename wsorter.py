#! /usr/bin/python3.1

# Print the order of a list of apache log files
import sys
import os.path
import io
import time

class timer:
    def __init__(self):
        self.old = time.time()
    def stop(self, msg):
        current = time.time()
        print('%20s .. %.2f sec' % (msg, current - self.old))
        self.old = current

# 01/Apr/2012:06:26:24 +0200
months = {'Jan': '01', 'Feb': '02', 'Mar': '03', 'Apr': '04', 'May': '05', 'Jun':'06',
          'Jul':'07', 'Aug':'08', 'Sep':'09', 'Oct': '10', 'Nov':'11', 'Dec':'12'}
(slice_day, slice_month, slice_year) = slice(2), slice(3, 6), slice(7, 11)
slice_hour, slice_min, slice_sec = slice(12, 14), slice(15, 17), slice(18, 20)
def parse(line):
    line, line, line = line.partition(' [')
    return line[slice_year], months[line[slice_month]], line[slice_day], line[slice_hour], line[slice_min], line[slice_sec]

#for i in range(1, 32):
#    print('%02d' % i)

def parseFile(f):
    fd = open(f)
    line = fd.readline()
    fd.close()
    try:
        year, month, day, hour, minute, second = parse(line)
        date = int(year + month + day + hour + minute + second)
    except:
        line = line.rstrip('\n')
        print('WARNING: %s: couldnt parse: \nWARNING:%s' % (f, line))
    return date

path = os.path.realpath(sys.argv[1])
newpath = path + '-new'
try:
    os.mkdir(newpath)
except:
    pass
l = os.listdir(path)
files = {}
for f in l:
    d = parseFile(os.path.join(path, f))
    n = f[:2]
#    print(d, f, n)
    if d in files:
        print('WARNING: the same key %d is inserted for file %s' % (d, f))
    else:
        files[d] = f
o = sorted(files)
t = timer()
for i in o:
    b = files[i]
    b = b[:2] + '.log'
#    print(i, '%10s %2s' % (files[i], b))
    cmd = 'cat %s >> %s' % (os.path.join(path, files[i]), os.path.join(newpath, b))
#    print(cmd)
    os.system(cmd)
    t.stop('%s -> %s' % (files[i], b))


