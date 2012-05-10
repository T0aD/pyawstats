#! /usr/bin/python3.1

# Print the order of a list of apache log files
import sys
import os.path
import io

# 01/Apr/2012:06:26:24 +0200
months = {'Jan': '01', 'Feb': '02', 'Mar': '03', 'Apr': '04', 'May': '05', 'Jun':'06',
          'Jul':'07', 'Aug':'08', 'Sep':'09', 'Oct': '10', 'Nov':'11', 'Dec':'12'}
(slice_day, slice_month, slice_year) = slice(2), slice(3, 6), slice(7, 11)
slice_hour, slice_min, slice_sec = slice(12, 14), slice(15, 17), slice(18, 20)
def parse(line):
    line, line, line = line.partition(' [')
    return line[slice_year], months[line[slice_month]], line[slice_day], line[slice_hour], line[slice_min], line[slice_sec]

# Read the dates
files = {}
for i in range(1, len(sys.argv)):
    try:
        f = sys.argv[i]
        fd = open(f)
        line = fd.readline()
        fd.close()
    except:
        print('ERROR: couldnt open file %s' % f)
        continue
    try:
        year, month, day, hour, minute, second = parse(line)
        date = int(year + month + day + hour + minute + second)
        if date in files:
            print('WARNING: the same key %d is inserted for file %s' % (date, f))
        else:
            files[date] = f        
    except:
        line = line.rstrip('\n')
        print('WARNING: %s: couldnt parse: \nWARNING:%s' % (f, line))
o = sorted(files)
for i in o:
    print(i, files[i])

