#!/bin/bash

. ./../../.env

CITY="amman"
YEAR=2014

# create data directory
mkdir $DATADIR/settlements
mkdir $DATADIR/settlements/$YEAR

## descarga
wget -O $DATADIR/settlements/$YEAR/GHS_BUILT_LDS${YEAR}_GLOBE_R2016A_54009_250_v1_0.zip http://cidportal.jrc.ec.europa.eu/ftp/jrc-opendata/GHSL/GHS_SMOD_POP_GLOBE_R2016A/GHS_SMOD_POP${YEAR}_GLOBE_R2016A_54009_1k/V1-0/GHS_SMOD_POP${YEAR}_GLOBE_R2016A_54009_1k_v1_0.zip

echo 'unzip'
unzip -o -d $DATADIR/settlements/$YEAR/ $DATADIR/settlements/$YEAR/GHS_SMOD_POP${YEAR}_GLOBE_R2016A_54009_1k_v1_0.zip

echo 'transform Countries'
ogr2ogr -t_srs EPSG:54009 ${DATADIR}/boundries/${CITY}_54009.shp ${DATADIR}/boundries/${CITY}.shp


echo 'cutting border'
gdalwarp -cutline ${DATADIR}/boundries/${CITY}_54009.shp -crop_to_cutline -dstalpha $DATADIR/settlements/$YEAR/GHS_SMOD_POP${YEAR}_GLOBE_R2016A_54009_1k_v1_0/*.tif ${DATADIR}/settlements/$YEAR/settlements_${CITY}_54009.tif

echo 'tranform'
gdalwarp  -s_srs EPSG:54009 -t_srs EPSG:4326 ${DATADIR}/settlements/$YEAR/settlements_${CITY}_54009.tif ${DATADIR}/settlements/$YEAR/settlements_${CITY}.tif

echo 'storing in db'
raster2pgsql -d -I -C -M -F -s 4326 ${DATADIR}/settlements/$YEAR/settlements_${CITY}.tif raw.${CITY}_settlements_${YEAR} > ${DATADIR}/settlements/$YEAR/settlements_${CITY}.sql

psql -d $PGDATABASE -h $PGHOST -U $POSTGRES_USER -f ${DATADIR}/settlements/$YEAR/settlements_${CITY}.sql
