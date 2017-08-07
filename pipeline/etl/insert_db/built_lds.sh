#!/bin/bash

. ./../../.env

CITY=$1
YEAR=$2

DATADIR=$3

### aqui
echo 'transform Countries'
ogr2ogr -t_srs EPSG:54009 ${DATADIR}/shp_buffer/${CITY}.shp ${DATADIR}/shp_buffer/${CITY}.shp

echo 'cutting border'
gdalwarp -cutline ${DATADIR}/shp_buffer/${CITY}.shp -crop_to_cutline -dstalpha $DATADIR/built_lds/$YEAR/GHS_BUILT_LDS${YEAR}_GLOBE_R2016A_54009_250_v1_0/*.tif ${DATADIR}/built_lds/$YEAR/built_lds_${CITY}_54009.tif

echo 'tranform'
gdalwarp  -s_srs EPSG:54009 -t_srs EPSG:4326 ${DATADIR}/built_lds/$YEAR/built_lds_${CITY}_54009.tif ${DATADIR}/built_lds/$YEAR/built_lds_${CITY}.tif

echo 'storing in db'
export PGPASSWORD=${PASSWORD} raster2pgsql -d -I -C -M -F -s 4326 ${DATADIR}/built_lds/$YEAR/built_lds_${CITY}.tif raw.${CITY}_built_lds_${YEAR} > ${DATADIR}/built_lds/$YEAR/built_lds_${CITY}.sql

psql -d $PGDATABASE -h $PGHOST -U $POSTGRES_USER -f ${DATADIR}/built_lds/$YEAR/built_lds_${CITY}.sql
