#!/bin/bash

. ./../../.env

CITY="amman"
COUNTRY="jordan"

# create data directory
mkdir $DATADIR/dem

# descarga
#### TODO

#unzip files into directory
#for z in $DATADIR/dem/*.zip; do unzip -o -d $DATADIR/dem/  $z; done
#
raster2pgsql -d -I -C -M -F -t 100x100 -s 4326 $DATADIR/dem/*_med075.tif raw.${CITY}_dem > $DATADIR/dem/elev.sql
psql -d $PGDATABASE -h $PGHOST -U $POSTGRES_USER -f $DATADIR/dem/elev.sql
