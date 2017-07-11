#!/bin/bash

. ./../../.env

CITY=$1

DATADIR=$2

#unzip files into directory
#for z in $DATADIR/dem/${CITY}/*.zip; do unzip -o -d $DATADIR/dem/${CITY}  $z; done

gdalwarp -cutline ${DATADIR}/boundries/${CITY}.shp -crop_to_cutline -dstalpha $DATADIR/dem/${CITY}/*_med075.tif  ${DATADIR}/dem/${CITY}/dem_${CITY}.tif

export PGPASSWORD=${PASSWORD} raster2pgsql -d -I -C -M -F -s 4326 $DATADIR/dem/${CITY}/dem_${CITY}.tif raw.${CITY}_dem > $DATADIR/dem/${CITY}/elev.sql
psql -d $PGDATABASE -h $PGHOST -U $POSTGRES_USER -f $DATADIR/dem/${CITY}/elev.sql
