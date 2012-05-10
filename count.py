#! /usr/bin/python3.1

import sqlite3
import sys

conn = sqlite3.connect(sys.argv[1])
cur = conn.cursor()

t = cur.execute('SELECT name FROM sqlite_master WHERE type = "table"')
for table in cur.fetchall():
    r = cur.execute('SELECT COUNT(*) FROM %s' % table[0])
    print(table[0], ': ', r.fetchone()[0], sep='')

