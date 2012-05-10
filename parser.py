#! /usr/bin/python3.1

import os
import sys
import io
import re
import geoip
import time

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

if not sys.argv[1].startswith('/'):
    logfile = os.path.realpath(os.getcwd() + '/' + sys.argv[1])
else:
    logfile = os.path.realpath(sys.argv[1])

class timer:
    def __init__(self, lines = 0):
        self.time, self.lines = (time.time(), lines)
    def stop(self, msg, lines):
        ct = time.time()
        dt = ct - self.time
        dl = lines - self.lines
#        print('%f - %f = %f' % (lines, self.lines, dl))
        self.lines = lines
        self.time = ct
        info = '%5d l/s' % (dl / dt,)
        if dt < 0.5:
            print('%s took %0.2f ms %s' % (msg, dt * 1000, info))
        else:
            print('%s took %0.2f s %s' % (msg, dt, info))

# BEGIN
class apparser:
    # Configuration
    months = {'Jan': '01', 'Feb': '02', 'Mar': '03', 'Apr': '04', 'May': '05', 'Jun':'06',
              'Jul':'07', 'Aug':'08', 'Sep':'09', 'Oct': '10', 'Nov':'11', 'Dec':'12'}
    notapage = {'css':1, 'js':1, 'class':1, 'gif':1, 'jpg':1, 'jpeg':1, 'png':1, 'bmp':1, 'swf':1}

    # Apache line pattern
    pattern = '^([^ ]+) ([^ ]+) ([^ ]+) ([^ ]+) \[(.+)\] "(.+)" ([^ ]+) ([^ ]+) "(.+|)" "(.+|)"$'
    pat_alt = '^([ ]+)([^ ]+) ([^ ]+) ([^ ]+) \[(.+)\] "(.+)" ([^ ]+) ([^ ]+) "(.+|)" "(.+|)"$'

    # Request line pattern (ie GET / HTTP/1.0)
    req_pattern = '([^ ]+) ([^ ]+) ([^ ]+)'

    pages = {}
    codes = {}
    exts = {}
    parsed = 0 # Number of lines parsed
    parsed_but_bad = 0

    # Parse apache line
    def feed(self, line):
#        self.parsed += 1

        # Reset all properties
        self.is_page = False
        self.ext = None

        # Parse the line
        m = re.match(self.pattern, line)
        if m == None: # bad format...
            # Special case, HTTP/1.1 request with a blank host:
            # echo -e "GET / HTTP/1.1\nHost:\r\n" | nc 217.73.17.12 80
            m = re.match(self.pat_alt, line)
            if m == None:
#                print('BAD:', line)
                self.parsed_but_bad += 1
                return False

# sillusprojet.lescigales.org 188.143.232.25 - - [25/Apr/2012:06:52:06 +0200] "GET /forum/newreply.php?tid=273 HTTP/1.0" 200 6685 "http://sillusprojet.lescigales.org/newreply.php?tid=273" "Mozilla/0.91 Beta (Windows)"
# segpachene.lescigales.org 193.49.248.69 - - [23/Apr/2012:07:34:14 +0200] "GET /intramessenger/distant/actions.php?a=20&iu=Mzg&is=MzQ1Mw&v=34&bi=MA&ip=MTAuMzguNzcuMTQ3& HTTP/1.0" 200 529 "-" ""

        vhost, ip, user, wtf, date, req, code, size, referer, uagent = m.groups()
        if vhost == ' ': # pat_alt was matched and no vhost was entered, so we need to correct that
            vhost = ''
#        return True
        # Warning: the +0200
        d, g = date.split(' ') # g stands for garbage
        date, hour, min, sec = d.split(':')
        day, month, year = date.split('/')
        month = self.months[month]
        date = year + month + day + hour + min + sec + g
        self.minsec, self.tz = (min + sec, g)

        # Req

        m = re.match(self.req_pattern, req)
        if m is None:
 #           print('couldnt parse req:', req, vhost)
            return False
        query, uri, proto = m.groups()

        # Get baseURI
        if not uri.find('?') == -1:
            old = uri
            uri, rest = uri.split('?', 1)
        else:
            rest = ''
        ext = None
        filename = os.path.basename(uri)
        if not filename == '' and not filename.find('.') == -1:
            g, ext = filename.rsplit('.', 1)

#        if not code in self.codes:
#            self.codes[code] = True
        if not ext is None:
            if not ext in self.exts:
                self.exts[ext] = True
            if not ext in self.notapage:
                self.is_page = True
#            if not self.pages.has_key(ext):
#                self.pages[ext] = True

        # Exporting data
        self.ext = ext
        self.db_id = year + month
        self.vhost, self.ip, self.user, self.wtf = (vhost, ip, user, wtf)
        self.referer, self.uagent = (referer, uagent)
        self.query, self.uri, self.rest, self.proto = (query, uri, rest, proto)
        self.code, self.size = (code, size)
        self.year, self.month, self.day, self.hour, self.date = (year, month, day, hour, date)
        return True

# END

parser = apparser()

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

class model_hits:
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
    data = {} # thats our stash baby!
    def __init__(self, db):
        self.table = self.__class__.__name__
        self.db = db
        self.SELECT = 'SELECT id FROM %s WHERE name = ?' % self.table
        self.INSERT = 'INSERT INTO %s (name) VALUES (?)' % self.table
        self.CREATE = "CREATE TABLE %s (id integer primary key, name text)" % self.table
        # Try to create the table
        try:
            db.execute(self.CREATE)
        except sqlite3.OperationalError as e:
            pass

    # Returns unique ID for the given name
    def get(self, name):
        # Fetch from local cache
        if name in self.data:
            return self.data[name]
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
    dbs = {}
    db = None
    CREATE = "CREATE TABLE %s (id integer primary key, name text)"
    DATABASE = 'apache_%s.sqlite'
    # Returns the connection to the database, create the database
    # if it doesnt exist
    def get(self, id):
 #       print(dbname)
#        exit()
#    def get(self, dbname):
        if not id in self.dbs:
            dbname = self.DATABASE % id
            print('%s doesnt exist' % dbname)
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
                            'country': countries(c)}
            c.commit()
        return self.dbs[id]

storer = storer()

# Reading logfile
try:
    fd = io.open(logfile, 'r')
except Exception as e:
    die(e)

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

protocols = {'HTTP/1.0':0, 'HTTP/1.1':1, 'default':0}

lineTimer = timer()
while True:
    if (i % 1001) == 0 and not i == 0:
        lineTimer.stop('parsed %10d lines' % i, i)
    if i > passes:
        break
    i += 1
    # We cannot strip the beginning of line (in case of empty vhost)

    line = fd.readline()
    if line == '': # EOF
        break
    line = line.rstrip("\n")
    r = parser.feed(line)
    if r == False:
        continue

    # Time identifiers
    month_id = parser.year + parser.month
    day_id = month_id + parser.day
    hour_id = day_id + parser.hour

#    dbname = 'apache_%s.sqlite' % parser.db_id
    d = storer.get(month_id)

    # Collect unique informations
    vhost_id = d['vhost'].get(parser.vhost)
    referer_id = d['referer'].get(parser.referer)
    uagent_id = d['uagent'].get(parser.uagent)
    ip_id = d['ip'].get(parser.ip)
    uri_id = d['uri'].get(parser.uri)
#    if parser.vhost != 'lescigales.org':
#        continue

    # Reset size to 0
    if parser.size == '-' or parser.code == '304':
        parser.size = 0
    size = int(parser.size) # for next uses

    # experimental
#    rest_id = d['rest'].get(parser.rest)
#    query_id = d['query'].get(parser.query)
#    tz_id = d['timezone'].get(parser.tz)
#    hour_id2 = d['hour'].get(hour_id)

    # Compressor ?
    if parser.proto in protocols:
        proto = protocols[parser.proto]
    else:
        proto = protocols['default']
    # Compressed line:
#    print(vhost_id, ip_id, parser.user, parser.wtf, 
#          hour_id2, parser.minsec, tz_id, query_id,
#          uri_id, rest_id, proto, parser.code, parser.size, referer_id, uagent_id)
#    continue

    # Increments hits
    # #########################
    cnt = collections.Counter({'hits': 1, 'traffic': size})
    for time_id in (month_id, day_id, hour_id):
        if not vhost_id in hits[time_id]:
            hits[time_id][vhost_id] = cnt
        else:
            hits[time_id][vhost_id] += cnt

    ### Insert Special (vhost 0 for global stat)
    for time_id in (month_id, day_id, hour_id):
        if not 0 in hits[time_id]:
            hits[time_id][0] = cnt
        else:
            hits[time_id][0] += cnt

    # Increments referers
    # #########################
    if not parser.referer == '-':
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

    # Countries
    # #########################
    countryName, countryCode = geoip.query(parser.ip)
    if not countryCode == None:
        country_id = d['country'].get(countryCode + '|' + countryName)
#        print(parser.ip, countryCode, countryName, country_id)

    # OS
    # #########################
    # Browser
    # #########################
    # Robots
    # #########################



print('stopped after', i, 'lines')

# Commit trial
for dbname in storer.dbs:
    print('commiting for', dbname, end='')
    sys.stdout.flush()
    d = storer.get(dbname)
    d['conn'].commit()
    print(' done')

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

# Saving hits
formatHITS = "INSERT INTO hits (vhost_id, date, frequency, hits, traffic) VALUES (?,?,?,?,?)"
formatHITS2 = 'SELECT hits, traffic FROM hits WHERE vhost_id = ? AND date = ?'
updateHITS = 'UPDATE hits SET hits = ?, traffic = ? WHERE vhost_id = ? AND date = ?'
frequencies = {6:'month', 8:'day', 10:'hour'}
print('saving hits... ', end='')
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
        res = r.fetchone()
        cnt = hits[time_id][vhost] # save the counter
        if res == None:
#            print('None to be found!')
            r = conn.execute(formatHITS, (vhost, time_id, frequency, 
                                          hits[time_id][vhost]['hits'], 
                                          hits[time_id][vhost]['traffic']))
        else:
            vhits, vtraffic = res
            cnt2 = collections.Counter({'hits':vhits, 'traffic':vtraffic})
#            print('to add:', hits[time_id][vhost]['hits'], hits[time_id][vhost]['traffic'])
#            print('existing', vhits, vtraffic)
#            print(cnt)
#            print(cnt2)
#            print(cnt+cnt2)
            cnt3 = cnt + cnt2
            r = conn.execute(updateHITS, (cnt3['hits'], cnt3['traffic'], vhost, time_id))
#            print('Found some entry already, updating..')
#        print('\t',time_id, vhost, hits[time_id][vhost])

    conn.commit()
print('done')
