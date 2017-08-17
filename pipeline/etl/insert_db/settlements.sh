#!/bin/bash

. ./.env

CITY=$1
YEAR=$2
DATADIR=$3

if [ ! -e ${DATADIR}/shp_buffer/${CITY}_54009.shp ]
then
        echo 'transform coordinate system'
        ogr2ogr -s_srs EPSG:4326 -t_srs EPSG:54009 ${DATADIR}/shp_buffer/${CITY}_54009.shp ${DATADIR}/shp_buffer/${CITY}.shp
fi

echo 'cutting border'
gdalwarp -cutline ${DATADIR}/shp_buffer/${CITY}_54009.shp -crop_to_cutline -dstalpha $DATADIR/settlements/$YEAR/GHS_SMOD_POP${YEAR}_GLOBE_R2016A_54009_1k_v1_0/*.tif ${DATADIR}/settlements/$YEAR/settlements_${CITY}_54009.tif

echo 'tranform'
gdalwarp  -s_srs EPSG:54009 -t_srs EPSG:4326 ${DATADIR}/settlements/$YEAR/settlements_${CITY}_54009.tif ${DATADIR}/settlements/$YEAR/settlements_${CITY}.tif

echo 'storing in db'
export PGPASSWORD=${POSTGRES_PASSWORD}; raster2pgsql -d -I -C -F -s 4326 ${DATADIR}/settlements/$YEAR/settlements_${CITY}.tif raw.${CITY}_settlements_${YEAR} > ${DATADIR}/settlements/$YEAR/settlements_${CITY}.sql

#psql -d $PGDATABASE -h $PGHOST -U $POSTGRES_USER -f ${DATADIR}/settlements/$YEAR/settlements_${CITY}.sql
