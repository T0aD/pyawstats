#! /usr/bin/env python3

import concurrent.futures
import urllib.request

URLS = [
    'cookbook', 'admin', 'www', 'forum', 'mail', 'sql', 'mail2',
    'www', 'toad', 'blog.toad'
    ]
URLS = [
    'http://docs.python.org',
    'http://medium.com',
    'http://www.tabrasco.com',
    'http://dispy.sourceforge.net/',
    'http://purecss.io',
    'http://www.reddit.com',
    'http://www.000webhost.com',
    'http://www.infowebmaster.fr',
    'http://nuclearelephant.com/',
    'http://www.free-h.org',
    'http://www.fur4x-hebergement.net'
    ] 

def load_url(url):
#    url = 'http://%s.lescigales.org/' % url
    data = urllib.request.urlopen(url).read()
    print(url, len(data))

import sys
try:
    workers = int(sys.argv[1])
except:
    workers = 1

print('max workers: %d' % workers)
with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
    for url in URLS:
        executor.submit(load_url, url)
#    print('end')

