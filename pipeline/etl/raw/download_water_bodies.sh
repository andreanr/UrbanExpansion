#!/bin/bash

. ./../../.env

CITY='amman'
COUNTRY='Jordan'

# create data directory
#mkdir ${DATADIR}/water_bodies

echo ${DATADIR}

# descarga
#curl curl 'https://ago-item-storage.s3-external-1.amazonaws.com/e750071279bf450cbd510454a80f2e63/World_Water_Bodies.lpk?X-Amz-Security-Token=FQoDYXdzEID%2F%2F%2F%2F%2F%2F%2F%2F%2F%2FwEaDKz8RaZFKuRL9w2E%2FCKcA5PaOSpDd4r7wVS%2FuO1NSjqgf1FlDd9BIvT4LRG2Us347WqicoaLfMkhGNrtme6YBJMBYcnMKH3RR8CghtU%2Bcop292Kv0LO0N90FdJ4n63x1nElF%2Fpc4Ajy32XlL4NwgD9gBmqfK6X1DKUvzSGa6iVMd%2BYCoztMCGf3sWifyjFidyEPZGr7b2ha2M3wSwmeeRYnTw83YUeTb1IinVl%2FQ1D5%2FNBMhP9JLwuOnF%2FYaLzYmfMQo9UZ%2B1XdkoK5RHZDnyBbXCYD7Mmb1hemmvnhwqYwiCFF8e4wSbyi1nZCfzDOnOT2BkWu2YpUWj5xY2n0jVPiAoRQRak%2F3HWr0zV8oEekYgeMBPlZCY6k9aypEMllthGpaM1c1t9UtzCbsr%2BtTtE5PpOS%2FqlmQrX%2F7yRVJXDbD2DmJ9Xfj1edQhd9WjpMu%2Bmv05hhwPMPCJaopiA42OWrxLkSh%2F2msoUzaTiULc9%2BHiQrFtze4bQFNJI6eYvKFDJ9PNbLtwvs3mLe9w3oOruMjhEA%2BbNxnfSdcuezrWiBUM4Ww8IUK9lRhDY4ooYPWygU%3D&X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Date=20170630T000314Z&X-Amz-SignedHeaders=host&X-Amz-Expires=300&X-Amz-Credential=ASIAI7KYOMUBNLCYDZAQ%2F20170630%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Signature=333daf53029a51aa600895573709deabae8f29dc2ecaf34d7d97ca51ec774c0a' -H 'Accept-Encoding: gzip, deflate, sdch, br' -H 'Accept-Language: en-US,en;q=0.8' -H 'Upgrade-Insecure-Requests: 1' -H 'User-Agent: Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36' -H 'Accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8' -H 'Referer: http://www.arcgis.com/home/item.html?id=e750071279bf450cbd510454a80f2e63' -H 'Connection: keep-alive' --compressed > $DATADIR/water_bodies/world_water_bodies.zip


# Unzip
#7z x $DATADIR/water_bodies/world_water_bodies.zip -oc:$DATADIR/water_bodies/ * -r

# Convert to shp
#ogr2ogr -f "ESRI Shapefile" ${DATADIR}/water_bodies/shps ${DATADIR}/water_bodies/v10/hydropolys.gdb

# clip
ogr2ogr -clipsrc ${DATADIR}/countries_shp/${COUNTRY}.shp ${DATADIR}/water_bodies/shps/${CITY}.shp ${DATADIR}/water_bodies/shps/hydropolys.shp

# upload to postgresql
shp2pgsql -s 4326 -d -D -I -W "latin1" ${DATADIR}/water_bodies/shps/${CITY}.shp raw.${CITY}_water_bodies | psql -d $PGDATABASE -h $PGHOST -U $POSTGRES_USER
