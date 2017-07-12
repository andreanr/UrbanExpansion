#!/bin/bash

. ./../../.env

CITY=$1

DATADIR=$2

# clip
ogr2ogr -clipsrc ${DATADIR}/boundries/${CITY}.shp ${DATADIR}/water_bodies/shps/${CITY}.shp ${DATADIR}/water_bodies/shps/hydropolys.shp

# upload to postgresql
shp2pgsql -s 4326 -d -D -I -W "latin1" ${DATADIR}/water_bodies/shps/${CITY}.shp raw.${CITY}_water_bodies | psql -d $PGDATABASE -h $PGHOST -U $POSTGRES_USER
