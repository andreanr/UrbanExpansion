#!/bin/bash

. ./../../.env

CITY="amman"
COUNTRY="jordan"

# create data directory
mkdir $DATADIR/highways/$CITY


#descarga
wget https://s3.amazonaws.com/metro-extracts.mapzen.com/${CITY}_${COUNTRY}.osm2pgsql-shapefiles.zip

#unzip files into directory
for z in *.zip; do unzip -o -d $DATADIR/highways/$CITY $z; done

#delete zip files
for z in *.zip; do rm $z; done

# upload to postgresql 
shp2pgsql -s 4326 -d -D -I -W "latin1" ${DATADIR}/highways/${CITY}/${CITY}_${COUNTRY}_osm_line.shp raw.${CITY}_highways | psql -d $PGDATABASE -h $PGHOST -U $POSTGRES_USER

