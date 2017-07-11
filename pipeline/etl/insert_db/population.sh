#!/bin/bash

. ./../../.env

CITY=$1
YEAR=$2

DATADIR=$3

echo 'cutting border'
gdalwarp -cutline ${DATADIR}/boundries/${CITY}_54009.shp -crop_to_cutline -dstalpha $DATADIR/population/$YEAR/GHS_POP_GPW4${YEAR}_GLOBE_R2015A_54009_250_v1_0/*.tif ${DATADIR}/population/$YEAR/population_${CITY}_54009.tif


eco 'tranform'
gdalwarp  -s_srs EPSG:54009 -t_srs EPSG:4326 ${DATADIR}/population/$YEAR/population_${CITY}_54009.tif ${DATADIR}/population/$YEAR/population_${CITY}.tif

echo 'storing in db'
export PGPASSWORD=${PASSWORD} raster2pgsql -d -I -C -M -F -s 4326 ${DATADIR}/population/$YEAR/population_${CITY}.tif raw.${CITY}_population_${YEAR} > ${DATADIR}/population/$YEAR/population_${CITY}.sql

psql -d $PGDATABASE -h $PGHOST -U $POSTGRES_USER -f ${DATADIR}/population/$YEAR/population_${CITY}.sql
