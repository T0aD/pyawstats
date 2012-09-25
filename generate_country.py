#! /usr/bin/python3.1

import sys
import io
import sqlite3
import csv

target = sys.argv[1]
fd = io.open(target)
content = csv.reader(fd)
db = sqlite3.connect('./geoip.sqlite')
countries = """
CREATE TABLE countries (begin_ip text, end_ip text, 
        begin_num integer, end_num integer,
        country text, name text);
CREATE INDEX c1 ON countries (begin_num);
CREATE INDEX c2 ON countries (end_num);
"""
insert = """
INSERT INTO countries (begin_ip, end_ip, begin_num, end_num, country, name)
VALUES (?, ?, ?, ?, ?, ?)
"""
try:
    db.executescript(countries)
except:
    pass

for line in content:
    bip, eip, bnum, enum, country, name = line
    db.execute(insert, (bip, eip, int(bnum), int(enum), country, name))
#    print(line)

db.commit()
print('*ding*') # <- thats supposed to play a microwave alert
