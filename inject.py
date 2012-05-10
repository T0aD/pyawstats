#! /usr/bin/python3.1

import sys
import os.path
import sqlite3

db = sqlite3.connect('queue.sqlite')
try:
    # id will be used for order of processing as well...
    db.execute('CREATE TABLE queue (id integer primary key, name text, done integer)')
except:
    pass

def add(db, logfile):
    SELECT = 'SELECT id FROM queue WHERE name = ?'
    INSERT = 'INSERT INTO queue (name, done) VALUES (?, 0)'
    r = db.execute(SELECT, (logfile, ))
    r = r.fetchone()
    if r is None:
        db.execute(INSERT, (logfile, ))
        db.commit()
        return True
    return False

# Of course this will crash if logs have space in their names, muhahaha:
for i in range(1, len(sys.argv)):
    f = sys.argv[i]
    if not os.path.exists(f):
        print('WARNING: file %s does not exist' % f)
        continue
    r = add(db, f)
    if r is False:
        print('WARNING: cannot add %s: already in database' % f)
        continue
    print(f, 'added')

