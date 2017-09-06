#!/bin/bash

. ./.env

CITY=$1
YEAR=$2
DATADIR=$3


echo 'storing in db'
export PGPASSWORD=${POSTGRES_PASSWORD}; raster2pgsql -d -I -C -F -s 4326 ${DATADIR}/settlements/$YEAR/settlements_${CITY}.tif raw.${CITY}_settlements_${YEAR} > ${DATADIR}/settlements/$YEAR/settlements_${CITY}.sql

#psql -d $PGDATABASE -h $PGHOST -U $POSTGRES_USER -f ${DATADIR}/settlements/$YEAR/settlements_${CITY}.sql
