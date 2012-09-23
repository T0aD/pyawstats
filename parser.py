#! /usr/bin/python3.1

import os
import sys
import io
import re
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
    passes = 40000

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

# BEGIN

import sqlite3

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
class queries(uniques): # GET / POST / HEAD
    pass

# Will not be used:
class rests(uniques): # rest of uris
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
        self.DATABASE = 'apache_%s.sqlite'
    # Returns the connection to the database, create the database
    # if it doesnt exist
    def get(self, id):
        if id in self.dbs:
            return self.dbs[id]
        dbname = self.DATABASE % id
        c = sqlite3.connect(dbname)
        cu = c.cursor()
        self.dbs[id] = {'conn': c, 'vhost': vhosts(c), 'referer': referers(c),
                        'uagent': uagents(c), 'ip': ips(c), 'uri': uris(c),
                        'query': queries(c)}
        return self.dbs[id]

    def getConnection(self, id):
        db = self.get(id)
        return db['conn']
    def write(self, id):
        d = self.get(id)
        for name in d:
            if name == 'conn':
                continue
            d[name].save()
    def commitAll(self):
        for db in self.dbs:
            self.dbs[db]['conn'].commit()
    def rollbackAll(self):
        for db in self.dbs:
            self.dbs[db]['conn'].rollback()

# END

storer = storer()

#hits = {}
import datetime
import collections
hits = collections.defaultdict(dict)
refs = collections.defaultdict(dict)
e404 = collections.defaultdict(dict)
stats404 = collections.defaultdict(dict)
codes = collections.defaultdict(dict)
statsExtensions = collections.defaultdict(dict)
statsWeekdays = collections.defaultdict(dict)
statsHours = collections.defaultdict(dict)
statsDays = collections.defaultdict(dict)
statsIP = collections.defaultdict(dict)
statsURI = collections.defaultdict(dict)

counter = collections.Counter

protocols = {'HTTP/1.0':0, 'HTTP/1.1':1, 'default':0}

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

import apparser
parser = apparser.apparser()


lineTimer = timer.timer()
sTimer = timer.timeit()
first = True
shit = []
weekday_cache = {}
updates = 0
inserts = 0
exceptions = 0
i = 0
for line in fd:
    if (i % 10001) == 0 and not i == 0:
        lineTimer.stop('parsed %10d lines' % i, i)
    if i > passes:
        break
    i += 1 # heresy!

    if gzipped is True:
        r = parser.feed(line.decode('ascii'))
    else:
        r = parser.feed(line) # 44,000 lines per sec

    if r == False:
        # Do we need to log those ? maybe so.. maybe so...
#        print("FALSE", line)
        continue

#    continue

    # Time identifiers
    month_id = parser.year + parser.month
    day_id = month_id + parser.day
    hour_id = day_id + parser.hour
    minute_id = hour_id + parser.min

    d = storer.get(month_id)

    # Load previous parsing results if any (only the first line)
    if first is True:
#        preload_hits(d, month_id, day_id, hour_id)
#        print(month_id, day_id, hour_id)
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

#    continue

    # Reset size to 0
    if parser.size == '-' or parser.code == '304':
        parser.size = 0
    size = int(parser.size) # for next uses

    # Reset referer to nothing
    if parser.referer == '-' or parser.referer == '':
        parser.referer = None

    # Page count
    page = int(parser.is_page)

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


    # URIs
    # #########################
    if page is 1:
        try:
            statsURI[month_id][vhost_id][uri_id]['hits'] += 1
            statsURI[month_id][vhost_id][uri_id]['traffic'] += size
            statsURI[month_id][vhost_id][uri_id]['size'] = size
        except KeyError:
            try:
                statsURI[month_id][vhost_id][uri_id] = {'hits': 1, 
                                                        'size': size, 'traffic': size}
            except KeyError:
                try:
                    statsURI[month_id][vhost_id] = {uri_id: {'hits': 1, 'size': size,
                                                             'traffic': size}}
                except KeyError:
                    raise

#    continue

    # Increments hits
    # #########################
    update_hits(vhost_id, size)

    ### Insert Special (vhost 0 for global stat)
    update_hits(0, size) # to move for post-processing (not main loop)

    # Increments referers
    # We save them only if the page was found ?
    # #########################
    if parser.referer is not None:
        try:
            refs[month_id][vhost_id][referer_id]['pages'] += page
            refs[month_id][vhost_id][referer_id]['hits'] += hits
        except:
            refs[month_id][vhost_id] = {referer_id: {'pages': page, 'hits': hits}}

    # Increment 404 errors
    # #########################
    if parser.code == '404':
        try:
            stats404[month_id][vhost_id][uri_id][referer_id]['hits'] += 1
        except:
            try:
                stats404[month_id][vhost_id][uri_id] = {referer_id: {'hits': 1}}
            except:
                stats404[month_id][vhost_id] = {uri_id: {referer_id: {'hits': 1}}}

    # Increments for HTTP codes
    # #########################

    try:
        codes[month_id][vhost_id][parser.code]['hits'] += 1
        codes[month_id][vhost_id][parser.code]['traffic'] += size
    except:
        codes[month_id][vhost_id] = {parser.code: {'hits': 1, 'traffic': size}}

    # Clients
    # #########################
    try:
        statsIP[month_id][vhost_id][ip_id]['hits'] += 1
        statsIP[month_id][vhost_id][ip_id]['traffic'] += size
        # I dont think its necessary to compare with old date:
        statsIP[month_id][vhost_id][ip_id]['last'] = minute_id 
        # pages?
        statsIP[month_id][vhost_id][ip_id]['pages'] += page
    except KeyError:
        try:
            statsIP[month_id][vhost_id][ip_id] = {'hits': 1, 'traffic': size, 'last': minute_id,
                                                  'pages': page}
        except KeyError:
            statsIP[month_id][vhost_id] = {ip_id: {'hits': 1, 'traffic': size,
                                                   'last': minute_id, 'pages': page}}

    # Month days
    # #########################
    try:
        statsDays[month_id][vhost_id][day_id]['hits'] += 1
        statsDays[month_id][vhost_id][day_id]['traffic'] += size
    except:
        statsDays[month_id][vhost_id] = {day_id: {'hits': 1, 'traffic': size}}

    # Hours
    # #########################
    try:
        statsHours[month_id][vhost_id][hour_id]['hits'] += 1
        statsHours[month_id][vhost_id][hour_id]['traffic'] += size
    except:
        statsHours[month_id][vhost_id] = {hour_id: {'hits': 1, 'traffic': size}}

    # Week days
    # #########################
    if day_id in weekday_cache:
        weekday = weekday_cache[day_id]
    else:
        weekday = datetime.date(int(parser.year), int(parser.month), int(parser.day)).weekday()
        weekday_cache[day_id] = weekday
    try:
        statsWeekdays[month_id][vhost_id][weekday]['hits'] += 1
        statsWeekdays[month_id][vhost_id][weekday]['traffic'] += size
    except:
        statsWeekdays[month_id][vhost_id] = {weekday: {'hits': 1, 'traffic': size}}

    # Visits
    # #########################

    # Filetypes
    # #########################
    if parser.ext is not None:
        try:
            statsExtensions[month_id][vhost_id][parser.ext]['hits'] += 1
            statsExtensions[month_id][vhost_id][parser.ext]['traffic'] += size
        except:
            statsExtensions[month_id][vhost_id] = {parser.ext: {'hits': 1, 'traffic': size}}

    #update_extensions(vhost_id, parser.ext, size)

    # OS
    # #########################
    # Browser
    # #########################
#    ua = hua.simple_detect(parser.uagent)
#    ua2 = hua.detect(parser.uagent)

    # Robots
    # #########################


#print('exceptions:', exceptions)

lineTimer.average()
sTimer.show('stopped after %d lines' % i)
#print('inserts', inserts, 'updates', updates)

# Commit trial
for dbname in storer.dbs:
    print('=' * 10, dbname)
    storer.write(dbname)
sTimer.show('storer.write()')


statsIPwrites = 0
for time_id in statsIP:
    for vhost_id in statsIP[time_id]:
        for ip in statsIP[time_id][vhost_id]:
            statsIPwrites += 1
            continue
            print(time_id, vhost_id, ip, statsIP[time_id][vhost_id][ip])
sTimer.show('statsIP writes: %d' % statsIPwrites)

statsDayswrites = 0
for time_id in statsDays:
    for vhost_id in statsDays[time_id]:
        for day in statsDays[time_id][vhost_id]:
            statsDayswrites += 1
            continue
            print(time_id, vhost_id, day, statsDays[time_id][vhost_id][day])
sTimer.show('statsDays writes: %d' % statsDayswrites)

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

class stats_base:
    def __init__(self, connection):
        print('created', self.__class__.__name__)
        self.stats = {'select': 0, 'insert': 0, 'update': 0}
        self.connection = connection
        self.SCHEMA = self.SCHEMA % self.TABLE
        self.INSERT = self.INSERT % self.TABLE
        self.SELECT = self.SELECT % self.TABLE
        self.UPDATE = self.UPDATE % self.TABLE
        self.INDEX = "CREATE INDEX %s_index_%d ON %s (%s)"
        try:
            indexes = self.INDEXES
        except AttributeError:
            indexes = []
        try:
            self.cursor().execute(self.SCHEMA)
            # Auto-generate indexes
            count = 1
            for index in indexes:
                self.cursor().execute(self.INDEX % (self.TABLE, count, self.TABLE, index))
        except sqlite3.OperationalError:
            pass
    def cursor(self):
        return self.connection.cursor()
    def select(self, **args):
        self.stats['select'] += 1
        return self.cursor().execute(self.SELECT, args).fetchone()
    def insert(self, **args):
        self.stats['insert'] += 1
        self.cursor().execute(self.INSERT, args)
    def update(self, **args):
        self.stats['update'] += 1
        self.cursor().execute(self.UPDATE, args)
    def save(self, **args):
        if self.select(**args) is None:
            self.insert(**args)
        else:
            self.update(**args)
    def changes(self):
        return "%s inserts: %d updates: %d" % (
            self.TABLE, self.stats['insert'], self.stats['update'])

# Writing extensions stats
######################################################################################
class stats_extensions(stats_base):
    TABLE = "stats_extensions"
    SCHEMA = """CREATE TABLE %s (vhost_id integer, 
                ext text, hits integer, traffic integer)"""
    INSERT = "INSERT INTO %s (vhost_id, ext, hits, traffic) VALUES (:vhost, :ext, :hits, :traffic)"
    UPDATE = "UPDATE %s SET hits = :hits, traffic = :traffic WHERE vhost_id = :vhost AND ext = :ext"
    SELECT = "SELECT * FROM %s WHERE vhost_id = :vhost AND ext = :ext"
    INDEXES = ['vhost_id, ext']

for time_id in statsExtensions:
    db = stats_extensions(storer.getConnection(time_id))
    for vhost_id in statsExtensions[time_id]:
        for ext in statsExtensions[time_id][vhost_id]:
            t = statsExtensions[time_id][vhost_id][ext]
            db.save(vhost=vhost_id, ext=ext, hits=t['hits'], traffic=t['traffic'])
    sTimer.show(db.changes())


# Writing URI stats
#####################################################################################
class stats_uris(stats_base):
    TABLE = "stats_uris"
    SCHEMA = """CREATE TABLE %s (vhost_id integer, 
                uri_id integer, size integer, traffic integer)"""
    INSERT = "INSERT INTO %s (vhost_id, uri_id, size, traffic) VALUES (:vhost, :uri, :size, :traffic)"
    UPDATE = "UPDATE %s SET size =:size, traffic =:traffic WHERE vhost_id = :vhost AND uri_id = :uri"
    SELECT = "SELECT * FROM %s WHERE vhost_id = :vhost AND uri_id = :uri"
    INDEXES = ['vhost_id, uri_id']

#storer.rollbackAll()
for time_id in statsURI:
    db = stats_uris(storer.getConnection(time_id))
    for vhost_id in statsURI[time_id]:
        for uri in statsURI[time_id][vhost_id]:
            t = statsURI[time_id][vhost_id][uri]
            db.save(vhost=vhost_id, uri=uri, size=t['size'], traffic=t['traffic'])

            continue
            if db.select(vhost=vhost_id, uri=uri) is None:
                db.insert(vhost=vhost_id, uri=uri, size=t['size'], traffic=t['traffic'])
            else:
                db.update(vhost=vhost_id, uri=uri, size=t['size'], traffic=t['traffic'])
    sTimer.show(db.changes())


# Writing codes stats
#####################################################################################
class stats_codes(stats_base):
    TABLE = "stats_codes"
    SCHEMA = """CREATE TABLE %s (vhost_id integer, 
                code text, hits integer, traffic integer)"""
    INSERT = "INSERT INTO %s (vhost_id, code, hits, traffic) VALUES (:vhost, :code, :hits, :traffic)"
    UPDATE = "UPDATE %s SET hits =:hits, traffic =:traffic WHERE vhost_id = :vhost AND code = :code"
    SELECT = "SELECT * FROM %s WHERE vhost_id = :vhost AND code = :code"
    INDEXES = ['vhost_id, code']

for time_id in codes:
    db = stats_codes(storer.getConnection(time_id))
    for vhost in codes[time_id]:
        for code in codes[time_id][vhost]:
            t = codes[time_id][vhost][code]
            db.save(vhost=vhost, code=code, hits=t['hits'], traffic=t['traffic'])
#            if not code in ('404', '200'):
#                continue
            continue
            print(time_id, vhost, code, codes[time_id][vhost][code])
    sTimer.show(db.changes())

# 404 stats
class stats_404(stats_base):
    TABLE = "stats_404"
    SCHEMA = """CREATE TABLE %s (vhost_id integer, uri_id integer, referer_id integer,
                hits integer)"""
    INSERT = """INSERT INTO %s (vhost_id, uri_id, referer_id, hits) 
                VALUES (:vhost, :uri, :referer, :hits)"""
    UPDATE = "UPDATE %s SET hits=:hits WHERE vhost_id=:vhost AND uri_id=:uri AND referer_id=:referer"
    SELECT = "SELECT * FROM %s WHERE vhost_id =:vhost AND uri_id=:uri AND referer_id =:referer"
    INDEXES = ['vhost_id, uri_id, referer_id']

for time_id in stats404:
    db = stats_404(storer.getConnection(time_id))
    for vhost in stats404[time_id]:
        for uri in stats404[time_id][vhost]:
            for referer in stats404[time_id][vhost][uri]:
                t = stats404[time_id][vhost][uri][referer]['hits']
                db.save(vhost=vhost, uri=uri, referer=referer, hits=t)
#                print('hits', stats404[time_id][vhost][uri][referer])
                continue
#            print(time_id, vhost, referer, e404[time_id][vhost][referer])

for time_id in refs:
    for vhost in refs[time_id]:
        for referer in refs[time_id][vhost]:
            continue
            print(time_id, vhost, referer, refs[time_id][vhost][referer])

# Writing hits stats
#####################################################################################
class stats_hits(stats_base):
    TABLE = "stats_hits"
    SCHEMA = """CREATE TABLE %s (vhost_id integer, date integer, frequency text, 
                hits integer, traffic integer)"""
    INSERT = """INSERT INTO %s (vhost_id, date, frequency, hits, traffic) VALUES 
                (:vhost, :date, :frequency, :hits, :traffic)"""
    UPDATE = "UPDATE %s SET hits =:hits, traffic =:traffic WHERE vhost_id = :vhost AND date =:date"
    SELECT = "SELECT * FROM %s WHERE vhost_id = :vhost AND date = :date"
    INDEXES = ['vhost_id, date']

# Saving hits
schema_hits = """
CREATE TABLE hits (
        date integer,
        frequency text,
        vhost_id integer,
        hits integer,
        traffic integer
)
"""
formatHITS = "INSERT INTO hits (vhost_id, date, frequency, hits, traffic) VALUES (?,?,?,?,?)"
formatHITS2 = 'SELECT hits, traffic FROM hits WHERE vhost_id = ? AND date = ?'
updateHITS = 'UPDATE hits SET hits = ?, traffic = ? WHERE vhost_id = ? AND date = ?'
frequencies = {6:'month', 8:'day', 10:'hour'}

for time_id in hits:
    # Get the month_id (to target the database where to write)
    month_id = time_id[0:6]
    # TODO: useless creation of object everytime, careful:
    db = stats_hits(storer.getConnection(month_id))
#    print(month_id, time_id, db)
    frequency = frequencies[len(time_id)]
#    print(month_id, time_id, len(time_id), frequencies[len(time_id)], dbname)
    for vhost in hits[time_id]:
        t = hits[time_id][vhost] # save the counter
        db.save(vhost=vhost, date=time_id, frequency=frequency, 
                 hits=t['hits'], traffic=t['traffic'])

        continue
        # check if does not exist already...
#        print('checking', vhost, time_id)
        r = conn.execute(formatHITS2, (vhost, time_id))
        res = r.fetchone()
        cnt = hits[time_id][vhost] # save the counter
        if res == None:
#            print('None to be found!')
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
            r = conn.execute(updateHITS, (cnt3['hits'], cnt3['traffic'], vhost, time_id))
#            print('Found some entry already, updating..')
#        print('\t',time_id, vhost, hits[time_id][vhost])

#    conn.commit()
    sTimer.show('%s %s' % (time_id, db.changes()))

#storer.rollbackAll()
storer.commitAll()
sTimer.show('storer.commitAll()')

