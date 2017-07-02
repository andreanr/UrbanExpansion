#!/bin/bash

. ./../../.env

CITY="amman"
COUNTRY="Jordan"
YEAR=2015

# create data directory
#mkdir $DATADIR/population
mkdir $DATADIR/population/$YEAR

# descarga

wget -O $DATADIR/population/$YEAR/GHS_POP_GPW4${YEAR}_GLOBE_R2015A_54009_250_v1_0.zip http://cidportal.jrc.ec.europa.eu/ftp/jrc-opendata/GHSL/GHS_POP_GPW4_GLOBE_R2015A/GHS_POP_GPW4${YEAR}_GLOBE_R2015A_54009_250/V1-0/GHS_POP_GPW4${YEAR}_GLOBE_R2015A_54009_250_v1_0.zip

#unzip files into directory
echo 'unzip'
unzip -o -d $DATADIR/population/$YEAR/ $DATADIR/population/$YEAR/GHS_POP_GPW4${YEAR}_GLOBE_R2015A_54009_250_v1_0.zip

echo 'transform to 4326'
gdalwarp  -s_srs EPSG:54009 -t_srs EPSG:4326: $DATADIR/population/$YEAR/GHS_POP_GPW4${YEAR}_GLOBE_R2015A_54009_250_v1_0/*.tif $DATADIR/population/$YEAR/population_tr.tif

echo 'cutting border'
gdalwarp -cutline ${DATADIR}/boundries/${COUNTRY}.shp -crop_to_cutline -dstalpha $DATADIR/population/$YEAR/population_tr.tif ${DATADIR}/population/$YEAR/population_${CITY}.tif 

echo 'storing in db'
raster2pgsql -d -I -C -M -F -t 100x100 -s 4326 ${DATADIR}/population/$YEAR/population_${CITY}.tif raw.${CITY}_population_${YEAR} > ${DATADIR}/population/$YEAR/population_${CITY}.sql

psql -d $PGDATABASE -h $PGHOST -U $POSTGRES_USER -f ${DATADIR}/population/$YEAR/population_${CITY}.sql

