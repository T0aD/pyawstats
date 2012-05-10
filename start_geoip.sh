#! /bin/sh

db="geoip.sqlite"

if [ -f "$db" ]; then
    echo "db file $db exists, aborting..."
    exit 1
fi


##
# Full path to GEOIP database
url="http://geolite.maxmind.com/download/geoip/database/GeoIPCountryCSV.zip"

wget $url

name=`basename $url`
echo "ZIP filename: $name"

# Collect the CSV filename
csv=`unzip -qql $name | awk '{print $NF}'`
echo "CSV found: $csv"

unzip $name
rm -v $name
./generate_country.py $csv
rm -v $csv
./count.py ./geoip.sqlite


