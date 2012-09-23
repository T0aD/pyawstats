#! /usr/bin/python3.1

# Use to extract parts of code from a file
# Will output everything between # BEGIN and # END

import sys
import io

if not len(sys.argv) == 2:
    print('syntax:', sys.argv[0], '<script>')
    exit()

inside = False
fd = io.open(sys.argv[1])
while True:

    line = fd.readline()
    if line == '':
        break
    line = line.rstrip('\n')
    if line == '# BEGIN':
        inside = True
        continue
    elif line == '# END':
        inside = False
        continue
    if inside == True:
        print(line)


