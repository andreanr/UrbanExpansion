#!/bin/bash

. ./.env

CITY=$1
DATADIR=$2

export PGPASSWORD=${POSTGRES_PASSWORD}; raster2pgsql -d -I -C -F -s 4326 $DATADIR/dem/${CITY}_dem.tif raw.${CITY}_dem > $DATADIR/dem/${CITY}_dem.sql

