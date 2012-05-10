#! /usr/bin/python3.1

import sqlite3
import math
dbname = 'geoip.sqlite'

class geoip:
    format = 'SELECT name, country FROM countries WHERE begin_num <= ? AND end_num >= ? LIMIT 1' 
    cache = {}
    def __init__(self, path):
        self.db = sqlite3.connect(path)
    def ip2long(self, ip):
        ip_t = ip.split('.')
        if not len(ip_t) == 4:
            return None
        longip = 0
        i = 3
        for ip in ip_t:
            longip += int(ip) * int(math.pow(256, i))
            i -= 1
        return longip
    def query(self, ip):
        if ip in self.cache:
            return self.cache[ip]
        longip = self.ip2long(ip)
        if longip is None:
            return None, None
        r = self.db.execute(self.format, (longip, longip))
        c = r.fetchone()
        if c is None:
            self.cache[ip] = (None, None)
            return None, None
        self.cache[ip] = c
        return c

# Lets test it bitch!
if __name__ == '__main__':
    import sys
    if not len(sys.argv) == 2:
        print("syntax:", sys.argv[0],"<ip address>")
        exit()
    g = geoip(dbname)
    ip = sys.argv[1]
    country, code = g.query(ip)
    print('country:', country, 'code:', code)


