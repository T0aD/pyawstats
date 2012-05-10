
import re
import os.path

class apparser:
    # Configuration
    months = {'Jan': '01', 'Feb': '02', 'Mar': '03', 'Apr': '04', 'May': '05', 'Jun':'06',
              'Jul':'07', 'Aug':'08', 'Sep':'09', 'Oct': '10', 'Nov':'11', 'Dec':'12'}
    notapage = {'css':1, 'js':1, 'class':1, 'gif':1, 'jpg':1, 'jpeg':1, 'png':1, 'bmp':1, 'swf':1}

    # Apache line pattern
    pattern = '^([^ ]+) ([^ ]+) ([^ ]+) ([^ ]+) \[(.+?)\] "(.+?)" ([^ ]+) ([^ ]+) "(.+?|)" "(.+?|)"$'
    pat_alt = '^([ ]+)([^ ]+) ([^ ]+) ([^ ]+) \[(.+?)\] "(.+?)" ([^ ]+) ([^ ]+) "(.+?|)" "(.+?|)"$'

    # Request line pattern (ie GET / HTTP/1.0)
    req_pattern = '([^ ]+) ([^ ]+)[ ]+([^ ]+)'

    pages = {}
    codes = {}
    exts = {}
    parsed = 0 # Number of lines parsed
    parsed_but_bad = 0

    # 
    # ##################
    miniPattern = '^([^ ]+) ([^ ]+) ([^ ]+) ([^ ]+) \[(.+?)\]'
    miniAlternative = '^([ ]+)([^ ]+) ([^ ]+) ([^ ]+) \[(.+?)\]'

    def __init__2(self):
        self.compiledMini1 = re.compile(self.miniPattern)
        self.compiledMini2 = re.compile(self.miniAlternative)

        self.compiled = re.compile('(%s|%s)' % (self.miniPattern, self.miniAlternative))

    def minifeed2(self, line):
        m = self.compiled.match(line)
#        m = self.compiledMini1.match(line)
        if m == None:
            return False
            m = self.compiledMini2.match(line)
            if m == None:
                return False
        print('lastindex:', m.lastindex)
        print('lastgroup:', m.lastgroup)
        print(m.groups())
        print('m.group(%d):' % m.lastindex, m.group(m.lastindex))

#        vhost, ip, user, wtf, date = m.groups()
        vhost, ip, user, wtf, date = m.group(m.lastindex)
        if vhost == ' ': # pat_alt was matched and no vhost was entered, so we need to correct that
            vhost = ''

    def minifeed(self, line):

        g, d = line.split('[', 1)
        d, g = d.split(' ', 1)

#        d, g = date.split(' ') # g stands for garbage
        date, hour, min, sec = d.split(':')
        day, month, year = date.split('/')
        month = self.months[month]

        self.year, self.month, self.day = (year, month, day)
        return None

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
        self.min, self.sec = (min, sec)
        self.minsec, self.tz = (min + sec, g)

        # Req

        m = re.match(self.req_pattern, req)
        if m is None:
            print('couldnt parse req:', req, vhost)
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

