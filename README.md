# pyawstats

Prototype of awstats replacement in Python (collect and generate graphs from Apache logs)

## Quick start

First generate the geoip.sqlite database of countries:

``` shell
$ ./start_geoip.sh
```

Then test it:

``` shell
$ ./geoip.py 207.97.227.239
country: United States code: US
```


Basic parser is in parser.py, happy reading !

