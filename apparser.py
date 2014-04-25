
import re
import os.path

# Kudos go to flox @freenode #python-fr
months = {'Jan': '01', 'Feb': '02', 'Mar': '03', 'Apr': '04', 'May': '05', 'Jun':'06',
          'Jul':'07', 'Aug':'08', 'Sep':'09', 'Oct': '10', 'Nov':'11', 'Dec':'12'}
def _parse(line):
    idx = line.find('[')
    return line[idx + 8:idx + 12], months[line[idx + 4:idx + 7]], line[idx + 1:idx + 3]
(slice_day, slice_month, slice_year) = slice(2), slice(3, 6), slice(7, 11)
def _parse2(line):
    line, line, line = line.partition(' [')
    return line[slice_year], months[line[slice_month]], line[slice_day]

import socket
class ipResolver():
    cache = {}
    def get(self, hostname):
        if hostname in self.cache:
            return self.cache[hostname]
        ret = socket.gethostbyname(hostname)
        self.cache[hostname] = ret
        return ret

class apparser:
    # Configuration
    months = {'Jan': '01', 'Feb': '02', 'Mar': '03', 'Apr': '04', 'May': '05', 'Jun':'06',
              'Jul':'07', 'Aug':'08', 'Sep':'09', 'Oct': '10', 'Nov':'11', 'Dec':'12'}
    notapage = {'css':1, 'js':1, 'class':1, 'gif':1, 'jpg':1, 
                'jpeg':1, 'png':1, 'bmp':1, 'swf':1, 'ico': 1}

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

    # Compile pattern
    def __init__(self):
        self.patternCompiled = re.compile(self.pattern)
        self.patternAltCompiled = re.compile(self.pat_alt)
        self.resolver = ipResolver()

        self.num = re.compile('^[0-9]')



    def proto(self, line):

        # separators:
        space = ' '
        bracket = '['
        dquote = '"'
        qmark = '?'
        dot = '.'
        slash = '/'

#        print('-=' * 20)
        vhost_off = line.find(space)
        if vhost_off == 0:
            self.vhost = ''
        else:
            self.vhost = line[:vhost_off]
#        print(line)
#        print('vhost:', self.vhost)

        ip_off = line.find(space, vhost_off + 1)
        self.ip = line[vhost_off + 1:ip_off]
#        print('ip:', self.ip)

#        vhost, ip, ident, user, date, req, code, size, referer, uagent = m.groups()
        ident_off = line.find(space, ip_off + 1)
        self.ident = line[ip_off+1:ident_off]
#        print('ident:', self.ident)

        user_off = line.find(space, ident_off + 1)
        self.user = line[ident_off+1:user_off]
#        print('user:', self.user)

        date_off = line.find(bracket, user_off + 1)
#        print('offsets:', user_off, date_off)
        # error parsing
        if date_off != (user_off + 1):
            print('PROBLEM')
# 92.60.60.181 - - [14/Jan/2014:17:10:41 +0100] "GET / HTTP/1.1" 200 4234 "-" "-"
        # date:
        self.day = line[date_off+1:date_off+3]
        self.month = line[date_off+4:date_off+7]
        self.year = line[date_off+8:date_off+12]
        self.hour = line[date_off+13:date_off+15]
        self.minute = line[date_off+16:date_off+18]
        self.second = line[date_off+19:date_off+21]
#        print(day, month, year, hour, minute, second)

        # request
        req_off = line.find(dquote, date_off + 21) + 1
        req_end_off = line.find(dquote, req_off)
        request = line[req_off:req_end_off]
#        print('request:', request)

#        query, uri, proto = m.groups()
        uri_off = request.find(space)
        self.query = request[:uri_off]
        protocol_off = request.rfind(space)
        self.uri = request[uri_off+1:protocol_off]
        self.protocol = request[protocol_off+1:]
#        print('query:', self.query)
#        print('uri:', uri)
#        print('protocol:', protocol)

        path_off = self.uri.find(qmark)
        if path_off == -1:
            path = self.uri
        else:
            path = self.uri[:path_off]
#        print('path:', path)
 
        extension = None
        filename = path[path.rfind(slash)+1:]
#        print('filename:', filename)

        ext_off = filename.rfind(dot)
        if not ext_off == -1:
            extension = filename[ext_off+1:]
#            extension = extension.lower()

#        if not extension is None and len(extension) > 4:
#            print('path:', path)
#            print('filename:', filename)
#            print('extension:', extension)

#        ext = None
#        if not filename == '' and not filename.find('.') == -1:
#            g, ext = filename.rsplit('.', 1)
#            ext = ext.lower()


        # code and size
        code_off = line.find(space, req_end_off + 2) 
        code = line[req_end_off+2:code_off]
#        print('code', code)
        size_off = line.find(space, code_off + 1)
        size = line[code_off+1:size_off]
#        print('size', size)

        # referer and user-agents
        referer_off = line.find(dquote, size_off) + 1
        referer_off_end = line.find(dquote, referer_off)
        referer = line[referer_off:referer_off_end]
        self.referer = referer
#        print('referer:', referer)

        uagent_off = line.find(dquote, referer_off_end + 1) + 1
        uagent_off_end = line.find(dquote, uagent_off)
        uagent = line[uagent_off:uagent_off_end]
#        print('user-agent:', uagent)

#        return (self.vhost, self.referer)

    # Parse apache line
    def feed(self, line):
#        self.parsed += 1
        # Reset all properties
        self.is_page = False
        self.ext = None

        # Parse the line
#        m = re.match(self.pattern, line)
        m = self.patternCompiled.match(line)
        if m == None: # bad format...
            # Special case, HTTP/1.1 request with a blank host:
            # echo -e "GET / HTTP/1.1\nHost:\r\n" | nc 217.73.17.12 80
#            m = re.match(self.pat_alt, line)
            m = self.patternAltCompiled.match(line)
            if m == None:
#                print('BAD:', line)
                self.parsed_but_bad += 1
                return False
# LogFormat "%V %h %l %u %t \"%r\" %>s %b \"%{Referer}i\" \"%{User-Agent}i\"" complete
# sillusprojet.lescigales.org 188.143.232.25 - - [25/Apr/2012:06:52:06 +0200] "GET /forum/newreply.php?tid=273 HTTP/1.0" 200 6685 "http://sillusprojet.lescigales.org/newreply.php?tid=273" "Mozilla/0.91 Beta (Windows)"
# segpachene.lescigales.org 193.49.248.69 - - [23/Apr/2012:07:34:14 +0200] "GET /intramessenger/distant/actions.php?a=20&iu=Mzg&is=MzQ1Mw&v=34&bi=MA&ip=MTAuMzguNzcuMTQ3& HTTP/1.0" 200 529 "-" ""

        vhost, ip, ident, user, date, req, code, size, referer, uagent = m.groups()
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
#            print('couldnt parse req:', req, vhost)
            return False
        query, uri, proto = m.groups()

        # Get baseURI
        if not uri.find('?') == -1:
            old = uri
            uri, rest = uri.split('?', 1)
        else:
            rest = ''
        ext = None
        
#        print('-' * 10, 'START')
#        print(uri[uri.rfind('/')+1:])
#        print(uri)
        filename = uri[uri.rfind('/')+1:] # faster than:
#        filename = os.path.basename(uri)
#        print(filename)
#        print('-' * 10, 'END')

        if not filename == '' and not filename.find('.') == -1:
            g, ext = filename.rsplit('.', 1)
#            if not ext.islower():
#                print('NOT LOWER:', ext, ext.lower())
            ext = ext.lower()

#        if not code in self.codes:
#            self.codes[code] = True

        if not ext is None:
            if not ext in self.exts:
                self.exts[ext] = True
            if not ext in self.notapage:
                self.is_page = True
#                print('is page', ext)
#            if not self.pages.has_key(ext):
#                self.pages[ext] = True

#        num = re.compile('^[0-9]')


        # Check if we logged the reverse host instead of real IP address
#        first = ord(ip[0])
#        if first < 49 or first > 57:
#            print('NOT AN IP:', ip)
#            pass
#            print('*', end='')
        
#        if not self.num.match(ip):
#            ipaddr = self.resolver.get(ip)
#            print('NOT AN IP:', ip, ipaddr)
#            print('NOT AN IP:', ip)
#            ip = ipaddr


#        if user != '-' or ident != '-':
#            print(vhost, user, ident)

 #       return (ext, year + month, vhost, ip, user, ident, referer, uagent, query, uri, rest, proto, code, size, year, month, day, hour, date)
#        return True

        # Exporting data (5000 lines/sec loss when done)
        self.ext = ext
        self.db_id = year + month
        self.vhost, self.ip, self.user, self.ident = (vhost, ip, user, ident)
        self.referer, self.uagent = (referer, uagent)
        self.query, self.uri, self.rest, self.proto = (query, uri, rest, proto)
        self.code, self.size = (code, size)
        self.year, self.month, self.day, self.hour, self.date = (year, month, day, hour, date)
        return True

