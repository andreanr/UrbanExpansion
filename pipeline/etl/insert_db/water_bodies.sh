#!/bin/bash

. ./.env

CITY=$1
DATADIR=$2

# upload to postgresql
shp2pgsql -s 4326 -d -D -I -W "latin1" ${DATADIR}/water_bodies/shps/${CITY}_water_bodies.shp raw.${CITY}_water_bodies > ${DATADIR}/water_bodies/water_bodies_${CITY}.sql

#psql -d $PGDATABASE -h $PGHOST -U $POSTGRES_USER
