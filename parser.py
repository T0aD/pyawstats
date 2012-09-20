#! /usr/bin/python3.1

import os
import sys
import io
import re
import geoip
import time
import gzip
import timer

import httpagentparser as hua

#sys.setcheckinterval(400000)

if len(sys.argv) < 2:
    print("syntax:", sys.argv[0], "<logfile>")
    exit()

if len(sys.argv) > 2:
    passes = int(sys.argv[2])
else:
    passes = 14000

def die(msg):
    print(msg)
    exit()
def working(msg):
    print(msg, end='')
    sys.stdout.flush()

if not sys.argv[1].startswith('/'):
    logfile = os.path.realpath(os.getcwd() + '/' + sys.argv[1])
else:
    logfile = os.path.realpath(sys.argv[1])

#codes = {}
#exts = {}

uagent_d = {}
vhost_d = {}
i = 0
total = 0

import sqlite3


schema_hits = """
CREATE TABLE hits (
        date integer,
        frequency text,
        vhost_id integer,
        hits integer,
        traffic integer
)
"""

class model_hits3434:
    CREATE = """
CREATE TABLE hits (
        date integer,
        frequency text,
        vhost_id integer,
        hits integer,
        traffic integer
)
"""
    def add():
        pass
    def insert():
        pass
    def select():
        pass

class uniques:
    def __init__(self, db):
        self.data = {} # thats our stash baby!
        self.table = self.__class__.__name__
        self.db = db
        self.SELECT = 'SELECT id FROM %s WHERE name = ?' % self.table
        self.INSERT = 'INSERT INTO %s (name) VALUES (?)' % self.table
        self.INSERT2 = 'INSERT INTO %s (id, name) VALUES (?, ?)' % self.table
        self.CREATE = "CREATE TABLE %s (id integer primary key, name text)" % self.table
        # Try to create the table
        try:
            db.execute(self.CREATE)
        except sqlite3.OperationalError as e:
            pass

        # We load all the data (speeds parsing)
        query = 'SELECT id, name FROM %s' % self.table
        c = db.cursor()
        c.execute(query)
        r = c.fetchall()

        id = 0 # in case our table is empty
        for id, name in r:
            self.data[name] = id
        # Allows us not to store IDs before that point:
        self.lastID = id

        # To be able to increment without querying sqlite:
        self.maxID = id
        print('LOADED %10s -> %10d records' % (self.table, self.maxID))

    # Returns unique ID for the given name
    def get(self, name):
        # Fetch from local cache
        if name in self.data:
            return self.data[name]
        else:
            self.maxID += 1
            self.data[name] = self.maxID
            return self.maxID

        # Fetch from database
        c = self.db.cursor()
        c.execute(self.SELECT, (name, ))
        r = c.fetchone()
        if r is None:
            # Inserting into database
            c.execute(self.INSERT, (name, ))
#            self.db.commit()
            c.execute(self.SELECT, (name, ))
            r = c.fetchone()
        # Inserting into local cache
        self.data[name] = r[0]
        return self.data[name]

    # Ohlala! no more need for autoincrement values! (33,000 lines per second saved)
    def save(self):
        print('SAVING %10s -> %10d records' % (self.table, self.maxID - self.lastID))
        currentID = self.lastID
        c = self.db.cursor()
        for name in self.data:
            currentID = self.data[name]
            if currentID > self.lastID:
                c.execute(self.INSERT2, (currentID, name))

class vhosts(uniques):
    pass
class referers(uniques):
    pass
class uagents(uniques):
    pass
class ips(uniques):
    pass
class uris(uniques): # base of uris (before ?)
    pass
class countries(uniques):
    pass

# Will not be used:
class rests(uniques): # rest of uris
    pass
class queries(uniques): # GET / POST / HEAD
    pass
class timezones(uniques): # +0200
    pass
class hours(uniques): # 2012042606
    pass

# Manage database creation...
class storer:
    def __init__(self):
        self.dbs = {}
        self.db = None
        self.CREATE = "CREATE TABLE %s (id integer primary key, name text)"
        self.DATABASE = 'apache_%s.sqlite'
    # Returns the connection to the database, create the database
    # if it doesnt exist
    def get(self, id):
        if id in self.dbs:
            return self.dbs[id]

        dbname = self.DATABASE % id
#        print('%s doesnt exist' % dbname)
        c = sqlite3.connect(dbname)
        self.dbs[id] = c

        cu = c.cursor()
        # Create structure of the database if necessary
        try:
            """
            for table in ('vhosts', 'referers', 'uagents', 'ips', 'uris', 'rests',
                          'queries', 'timezones', 'hours'):
                          schema = self.CREATE % table
                    try:
                        cu.execute(schema)
                        print('created', table)
                    except:
                        pass
                        """
            # Create other tables:
            cu.execute(schema_hits)
            pass
        except sqlite3.OperationalError as e:
            pass
#            c.commit()
            """
            self.dbs[id] = {'conn': c, 'vhost': vhosts(c), 'referer': referers(c),
                               'uagent': uagents(c), 'ip': ips(c), 'uri': uris(c),
                                'rest': rests(c), 'query': queries(c), 'timezone': timezones(c),
                                'hour': hours(c)}
                                """
        self.dbs[id] = {'conn': c, 'vhost': vhosts(c), 'referer': referers(c),
                        'uagent': uagents(c), 'ip': ips(c), 'uri': uris(c),
                        'query': queries(c)}
        c.commit()
        return self.dbs[id]

    def commit(self, id):
        d = self.get(id)
        for name in d:
            if name == 'conn':
                continue
            d[name].save()
        d['conn'].commit()

storer = storer()

geoip = geoip.geoip('geoip.sqlite')

#hits = {}
import datetime
import collections
hits = collections.defaultdict(dict)
refs = collections.defaultdict(dict)
e404 = collections.defaultdict(dict)
codes = collections.defaultdict(dict)
statsExtensions = collections.defaultdict(dict)
statsWeekdays = collections.defaultdict(dict)
statsHours = collections.defaultdict(dict)
statsDays = collections.defaultdict(dict)
statsIP = collections.defaultdict(dict)

counter = collections.Counter

protocols = {'HTTP/1.0':0, 'HTTP/1.1':1, 'default':0}

# Test
#hits = {}
def update_hits2(vhost_id, size):
    global hits
    for time_id in (month_id, day_id, hour_id):
        try:
            hits[time_id][vhost_id]['hits'] += 1
            hits[time_id][vhost_id]['traffic'] += size
        except KeyError:
            try:
                hits[time_id][vhost_id] = {'hits': 1, 'traffic': size}
            except KeyError:
#                print('first!', time_id)
                hits[time_id] = {}
                hits[time_id][vhost_id] = {'hits': 1, 'traffic': size}


def update_hits(vhost_id, size):
    global hits
    for time_id in (month_id, day_id, hour_id):
        try:
            hits[time_id][vhost_id]['hits'] += 1
            hits[time_id][vhost_id]['traffic'] += size
        except KeyError:
            hits[time_id][vhost_id] = {'hits': 1, 'traffic': size}


# Reading logfile
try:
    if logfile.endswith('.gz'):
        fd = gzip.open(logfile, 'r')
        gzipped = True
    else:
        fd = io.open(logfile, 'r')
        gzipped = False
except Exception as e:
    die(e)

#parser = apparser()
import apparser
parser = apparser.apparser()


lineTimer = timer.timer()
sTimer = timer.timeit()
first = True
shit = []
country_cache = {}
updates = 0
inserts = 0
for line in fd:
    if (i % 10001) == 0 and not i == 0:
        lineTimer.stop('parsed %10d lines' % i, i)
    if i > passes:
        break
    i += 1 # heresy!

    if gzipped is True:
        r = parser.feed(line.decode('ascii'))
    else:
        r = parser.feed(line)
    if r == False:
        # Do we need to log those ? maybe so.. maybe so...
#        print("FALSE", line)
        continue

    # Time identifiers
    month_id = parser.year + parser.month
    day_id = month_id + parser.day
    hour_id = day_id + parser.hour

    d = storer.get(month_id)


    # Load previous parsing results if any (only the first line)
    if first is True:
#        preload_hits(d, month_id, day_id, hour_id)
        print(month_id, day_id, hour_id)
        first = False

    # Collect unique informations
    vhost_id = d['vhost'].get(parser.vhost)
    ip_id = d['ip'].get(parser.ip)
    referer_id = d['referer'].get(parser.referer)
    uagent_id = d['uagent'].get(parser.uagent)
    uri_id = d['uri'].get(parser.uri)
    query_id = d['query'].get(parser.query)

    # experimental
#    rest_id = d['rest'].get(parser.rest)
#    tz_id = d['timezone'].get(parser.tz)
#    hour_id2 = d['hour'].get(hour_id)

    # Reset size to 0
    if parser.size == '-' or parser.code == '304':
        parser.size = 0
    size = int(parser.size) # for next uses

    # Reset referer to nothing
    if parser.referer == '-' or parser.referer == '':
        parser.referer = None


    # Compressor ?
    if parser.proto in protocols:
        proto = protocols[parser.proto]
    else:
        proto = protocols['default']
    # Compressed line:
#    print(vhost_id, ip_id, parser.ident, parser.user, 
#          hour_id2, parser.minsec, tz_id, query_id,
#          uri_id, rest_id, proto, parser.code, parser.size, referer_id, uagent_id)
#    continue


    update_hits(vhost_id, size)
    continue
    """
    for time_id in (month_id, day_id, hour_id):
        try:
            hits[time_id][vhost_id]['hits'] += 1
            hits[time_id][vhost_id]['traffic'] += size
        except KeyError:
            hits[time_id][vhost_id] = {'hits': 1, 'traffic': size}
    continue
    """

    # Increments hits
    # #########################
    cnt = collections.Counter({'hits': 1, 'traffic': size})
#    cnt = counter({'hits': 1, 'traffic': size})
    for time_id in (month_id, day_id, hour_id):
#        print(i, time_id)
        if not vhost_id in hits[time_id]:
            hits[time_id][vhost_id] = cnt
#            inserts += 1
#            hits[time_id][vhost_id] = {'hits': 1, 'traffic': size}
#            print(hits[time_id][vhost_id])
        else:
            hits[time_id][vhost_id] += cnt
#            updates += 1
#            hits[time_id][vhost_id]['hits'] += 1
#            hits[time_id][vhost_id]['traffic'] += size
            
    continue

    cnt = collections.Counter({'hits': 1, 'traffic': size})
    ### Insert Special (vhost 0 for global stat)
    for time_id in (month_id, day_id, hour_id):
        if not 0 in hits[time_id]:
            hits[time_id][0] = cnt
        else:
            hits[time_id][0] += cnt

    # Increments referers
    # #########################
    if not parser.referer is None:
        if parser.is_page == True:
            cnt = collections.Counter({'pages': 1, 'hits': 1})
        else:
            cnt = collections.Counter({'hits': 1})
        for time_id in (month_id,):
#        for time_id in (month_id, day_id, hour_id):
            if not vhost_id in refs[time_id]:
                refs[time_id][vhost_id] = {}
            if not referer_id in refs[time_id][vhost_id]:
                refs[time_id][vhost_id][referer_id] = cnt
            else:
                refs[time_id][vhost_id][referer_id] += cnt

    # Increment 404 errors
    # #########################
    if parser.code == '404':
        cnt = collections.Counter({'hits':1})
        if not vhost_id in e404[month_id]:
            e404[month_id][vhost_id] = {}
#        if parser.referer is None:
#            pass
        if not referer_id in e404[month_id][vhost_id]:
            e404[month_id][vhost_id][referer_id] = cnt
        else:
            e404[month_id][vhost_id][referer_id] += cnt

    # Increments for HTTP codes
    # #########################
    cnt = collections.Counter({'hits':1, 'traffic':size})
    if not vhost_id in codes[month_id]:
        codes[month_id][vhost_id] = {}
    if not parser.code in codes[month_id][vhost_id]:
        codes[month_id][vhost_id][parser.code] = cnt
    else:
        codes[month_id][vhost_id][parser.code] += cnt

    # Clients
    # #########################
    cnt = collections.Counter({'hits':1, 'traffic':size})
    if not vhost_id in statsIP[month_id]:
        statsIP[month_id][vhost_id] = {}
    if not ip_id in statsIP[month_id][vhost_id]:
        statsIP[month_id][vhost_id][ip_id] = cnt
    else:
        statsIP[month_id][vhost_id][ip_id] += cnt


    # Month days
    # #########################
    cnt = collections.Counter({'hits':1, 'traffic':size})
    if not vhost_id in statsDays[month_id]:
        statsDays[month_id][vhost_id] = {}
    if not parser.day in statsDays[month_id][vhost_id]:
        statsDays[month_id][vhost_id][parser.day] = cnt
    else:
        statsDays[month_id][vhost_id][parser.day] += cnt

    # Hours
    # #########################
    cnt = collections.Counter({'hits':1, 'traffic':size})
    if not vhost_id in statsHours[month_id]:
        statsHours[month_id][vhost_id] = {}
    if not parser.hour in statsHours[month_id][vhost_id]:
        statsHours[month_id][vhost_id][parser.hour] = cnt
    else:
        statsHours[month_id][vhost_id][parser.hour] += cnt

    # Week days
    # #########################
    weekday = datetime.date(int(parser.year), int(parser.month), int(parser.day)).weekday()
    cnt = collections.Counter({'hits':1, 'traffic':size})
    if not vhost_id in statsWeekdays[month_id]:
        statsWeekdays[month_id][vhost_id] = {}
    if not weekday in statsWeekdays[month_id][vhost_id]:
        statsWeekdays[month_id][vhost_id][weekday] = cnt
    else:
        statsWeekdays[month_id][vhost_id][weekday] += cnt


    # Visits
    # #########################

    # Filetypes
    # #########################
    if not parser.ext == None:
        cnt = collections.Counter({'hits':1, 'traffic':size})
        if not vhost_id in statsExtensions[month_id]:
            statsExtensions[month_id][vhost_id] = {}
        if not parser.ext in statsExtensions[month_id][vhost_id]:
            statsExtensions[month_id][vhost_id][parser.ext] = cnt
        else:
            statsExtensions[month_id][vhost_id][parser.ext] += cnt

    # We should do that in a different process to improve the speed of log parsing:

    # Countries
    # #########################
    continue
    if parser.ip not in country_cache:
#        print('resolving for', parser.ip)
        countryName, countryCode = geoip.query(parser.ip)
        if not countryCode == None:
            country_id = d['country'].get(countryCode + '|' + countryName)
        else:
            country_id = 0
        country_cache[parser.ip] = country_id
    else:
        country_id = country_cache[parser.ip]
#        print(parser.ip, countryCode, countryName, country_id)

    # OS
    # #########################
    # Browser
    # #########################
#    ua = hua.simple_detect(parser.uagent)
#    ua2 = hua.detect(parser.uagent)

    # Robots
    # #########################


print('stopped after', i, 'lines')
lineTimer.average()
sTimer.show('end of parsing')

print('inserts', inserts, 'updates', updates)

# Commit trial
for dbname in storer.dbs:
    print('=' * 10, dbname)
    storer.commit(dbname)

#exit()
for time_id in statsIP:
    for vhost_id in statsIP[time_id]:
        for ip in statsIP[time_id][vhost_id]:
            continue
            print(time_id, vhost_id, ip, statsIP[time_id][vhost_id][ip])

for time_id in statsDays:
    for vhost_id in statsDays[time_id]:
        for day in statsDays[time_id][vhost_id]:
            continue
            print(time_id, vhost_id, day, statsDays[time_id][vhost_id][day])

for time_id in statsHours:
    for vhost_id in statsHours[time_id]:
        for hour in statsHours[time_id][vhost_id]:
            continue
            print(time_id, vhost_id, hour, statsHours[time_id][vhost_id][hour])

for time_id in statsWeekdays:
    for vhost_id in statsWeekdays[time_id]:
        for weekday in statsWeekdays[time_id][vhost_id]:
            continue
            print(time_id, vhost_id, weekday, statsWeekdays[time_id][vhost_id][weekday])

for time_id in statsExtensions:
    for vhost_id in statsExtensions[time_id]:
        for ext in statsExtensions[time_id][vhost_id]:
            continue
            print(time_id, vhost_id, ext, statsExtensions[time_id][vhost_id][ext])

for time_id in codes:
    for vhost in codes[time_id]:
        for code in codes[time_id][vhost]:
            if not code in ('404', '200'):
                continue
                print(time_id, vhost, code, codes[time_id][vhost][code])

for time_id in e404:
    for vhost in e404[time_id]:
        for referer in e404[time_id][vhost]:
            continue
            print(time_id, vhost, referer, e404[time_id][vhost][referer])

for time_id in refs:
    for vhost in refs[time_id]:
        for referer in refs[time_id][vhost]:
            continue
            print(time_id, vhost, referer, refs[time_id][vhost][referer])

#exit()

# Saving hits
formatHITS = "INSERT INTO hits (vhost_id, date, frequency, hits, traffic) VALUES (?,?,?,?,?)"
formatHITS2 = 'SELECT hits, traffic FROM hits WHERE vhost_id = ? AND date = ?'
updateHITS = 'UPDATE hits SET hits = ?, traffic = ? WHERE vhost_id = ? AND date = ?'
frequencies = {6:'month', 8:'day', 10:'hour'}
sTimer.show('SAVING HITS')
working('saving hits... ')
selects = 0
updates = 0
inserts = 0
for time_id in hits:
    # Get the month_id (to target the database where to write)
    month_id = time_id[0:6]
#    dbname = 'apache_%s.sqlite' % month_id
    frequency = frequencies[len(time_id)]
#    print(month_id, time_id, len(time_id), frequencies[len(time_id)], dbname)
    d = storer.get(month_id)
    conn = d['conn']
    for vhost in hits[time_id]:
        # check if does not exist already...
#        print('checking', vhost, time_id)
        r = conn.execute(formatHITS2, (vhost, time_id))
        selects += 1
        res = r.fetchone()
        cnt = hits[time_id][vhost] # save the counter
        if res == None:
#            print('None to be found!')
            inserts += 1
#            print('inserting', vhost, time_id, frequency)
            r = conn.execute(formatHITS, (vhost, time_id, frequency, 
                                          hits[time_id][vhost]['hits'], 
                                          hits[time_id][vhost]['traffic']))
        else:
            vhits, vtraffic = res
            #cnt2 = collections.Counter({'hits':vhits, 'traffic':vtraffic})
#            cnt2 = {'hits':vhits, 'traffic':vtraffic}
#            print('to add:', hits[time_id][vhost]['hits'], hits[time_id][vhost]['traffic'])
#            print('existing', vhits, vtraffic)
#            print(cnt)
#            print(cnt2)
#            print(cnt+cnt2)

            #cnt3 = cnt + cnt2
            cnt3 = {'hits': vhits + hits[time_id][vhost]['hits'], 
                    'traffic': vtraffic + hits[time_id][vhost]['traffic']}
            updates += 1
            r = conn.execute(updateHITS, (cnt3['hits'], cnt3['traffic'], vhost, time_id))
#            print('Found some entry already, updating..')
#        print('\t',time_id, vhost, hits[time_id][vhost])

    conn.commit()
print('done')
print('selects', selects, 'inserts', inserts, 'updates', updates)
sTimer.show('saved %d hits' % selects)
