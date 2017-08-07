#!/bin/bash

. ./../../.env

CITY=$1
YEAR=$2

DATADIR=$3

# Cut raster to shp
for z in *.tif; do
        gdalwarp -cutline ${DATADIR}/shp_buffer/${CITY}.shp -crop_to_cutline -dstalpha $z ${DATADIR}/city_lights/$YEAR/${z}_${CITY}.tif
        break 1
done

# upload raster to db
raster2pgsql -d -I -C -M -F  -s 4326 ${DATADIR}/city_lights/$YEAR/*_${CITY}.tif raw.${CITY}_city_lights_${YEAR} > ${DATADIR}/city_lights/$YEAR/lights_${CITY}.sql
psql -d $PGDATABASE -h $PGHOST -U $POSTGRES_USER -f ${DATADIR}/city_lights/$YEAR/lights_${CITY}.sql
