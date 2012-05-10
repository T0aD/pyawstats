#! /usr/bin/python3.1

# Try to detect a spamming bot from an apache log...
import sys
import apparser
import os.path
import io
import geoip

p = apparser.apparser()
g = geoip.geoip('geoip.sqlite')
files = {}
if not len(sys.argv) == 2:
    sys.stderr.write('Syntax: %s <apache logfile>' % sys.argv[0])
    exit()
if not os.path.exists(sys.argv[1]):
    sys.stderr.write('file %s does not exist' % sys.argv[1])
    exit()
fd = io.open(sys.argv[1])
i = 0
import collections
posts = collections.defaultdict(dict)
uris = collections.defaultdict(dict)
while True:
    i += 1
    line = fd.readline()
    if line == '': # EOF
        break
    line = line.rstrip('\n')
    if p.feed(line) == False:
#        sys.stderr.write('PARSE ERROR (%d): %s\n' % (i, line))
        continue
    if p.ip == '217.73.17.12':
        continue
    if p.query == 'POST':
        cnt = collections.Counter({'hits':1})
        if not p.vhost in posts[p.ip]:
            posts[p.ip][p.vhost] = cnt
        else:
            posts[p.ip][p.vhost] += cnt
#        print(p.vhost, p.ip, p.uri)
        url = p.vhost + p.uri
        if not url in uris[p.ip]:
            uris[p.ip][url] = cnt
        else:
            uris[p.ip][url] += cnt

fd.close()

for ip in posts:
    if len(posts[ip]) > 2: # This host posts on more than X vhosts
        for vhost in posts[ip]:
            print(ip, len(posts[ip]), vhost, posts[ip][vhost]['hits'], sep='\t')
        # Listing all the full urls
        for url in uris[ip]:
            print('-'*10, url, uris[ip][url]['hits'], sep='\t')
        # Last: IP localization
        cName, cCode = g.query(ip)
        print('IP INFORMATION', cName, cCode, sep='\t')
        print('-' * 60)
