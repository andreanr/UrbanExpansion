#!/bin/bash

TMPDIR="/shapefiles/"
STATEDIR="/amman_jordan/"
TABLA="Jordan"
STATESCHEMA="raw"
CITY="amman"

DB="postgis_test"
USER_NAME="andreuboadadeatela"

mkdir $TMPDIR
cd $TMPDIR

# amman
mkdir $STATEDIR
cd $STATEDIR

#descarga
wget https://s3.amazonaws.com/metro-extracts.mapzen.com/amman_jordan.osm2pgsql-shapefiles.zip
wget https://s3.amazonaws.com/metro-extracts.mapzen.com/amman_jordan.osm2pgsql-geojson.zip

#unzip files into directory
for z in *.zip; do unzip -o -d $TMPDIR $z; done

#delete zip files
for z in *.zip; do rm $z; done

shp2pgsql -s 4326 -S -W "utf-8" -I -d amman_jordan_osm_line.shp $STATESCHEMA.${CITY}_highways | psql -U $USER_NAME -d $DB
