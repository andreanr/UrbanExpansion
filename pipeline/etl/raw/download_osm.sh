#!/bin/bash

source ../../.env



curl 'http://planet.openstreetmap.org/planet/full-history/history-latest.osm.bz2' -H 'Accept-Encoding: gzip, deflate, sdch' -H 'Accept-Language: en-US,en;q=0.8' -H 'Upgrade-Insecure-Requests: 1' -H 'User-Agent: Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36' -H 'Accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8' -H 'Referer: http://planet.openstreetmap.org/planet/full-history/' -H 'Connection: keep-alive' --compressed >  $DATADIR/prueba
