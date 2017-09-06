#!/bin/bash

. ./.env

CITY=$1
YEAR=$2
DATADIR=$3


# upload raster to db
raster2pgsql -d -I -C -F -s 4326  ${DATADIR}/city_lights/$YEAR/*_${CITY}.tif  raw.${CITY}_city_lights_${YEAR}  >  ${DATADIR}/city_lights/$YEAR/city_lights_${CITY}.sql
#psql -d $PGDATABASE -h $PGHOST -U $POSTGRES_USER -f ${DATADIR}/city_lights/$YEAR/lights_${CITY}.sql
