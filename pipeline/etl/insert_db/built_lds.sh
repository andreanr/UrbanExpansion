#!/bin/bash

. ./.env

CITY=$1
YEAR=$2
DATADIR=$3


echo 'storing in db'
export PGPASSWORD=${POSTGRES_PASSWORD}; raster2pgsql -d -I -C -F -s 4326 ${DATADIR}/built_lds/$YEAR/built_lds_${CITY}.tif raw.${CITY}_built_lds_${YEAR} > ${DATADIR}/built_lds/$YEAR/built_lds_${CITY}.sql

#psql -d $PGDATABASE -h $PGHOST -U $POSTGRES_USER -f ${DATADIR}/built_lds/$YEAR/built_lds_${CITY}.sql
