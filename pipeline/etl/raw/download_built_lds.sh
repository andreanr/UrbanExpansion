#!/bin/bash

. ./../../.env

CITY="amman"
COUNTRY="Jordan"
YEAR=2000

# create data directory
#mkdir $DATADIR/built_lds
mkdir $DATADIR/built_lds/$YEAR

# descarga
wget -O $DATADIR/built_lds/$YEAR/GHS_BUILT_LDS${YEAR}_GLOBE_R2016A_54009_250_v1_0.zip http://cidportal.jrc.ec.europa.eu/ftp/jrc-opendata/GHSL/GHS_BUILT_LDSMT_GLOBE_R2015B/GHS_BUILT_LDS${YEAR}_GLOBE_R2016A_54009_250/V1-0/GHS_BUILT_LDS${YEAR}_GLOBE_R2016A_54009_250_v1_0.zip

#unzip files into directory
echo 'unzip'
unzip -o -d $DATADIR/built_lds/$YEAR/ $DATADIR/built_lds/$YEAR/GHS_BUILT_LDS${YEAR}_GLOBE_R2016A_54009_250_v1_0.zip

echo 'transform to 4326'
gdalwarp  -s_srs EPSG:54009 -t_srs EPSG:4326 -ovr 1 $DATADIR/built_lds/$YEAR/GHS_BUILT_LDS${YEAR}_GLOBE_R2016A_54009_250_v1_0/*.tif $DATADIR/built_lds/$YEAR/built_lds_tr.tif

echo 'cutting border'
gdalwarp -cutline ${DATADIR}/boundries/${COUNTRY}.shp -crop_to_cutline -dstalpha $DATADIR/built_lds/$YEAR/built_lds_tr.tif ${DATADIR}/built_lds/$YEAR/built_lds_${CITY}.tif

echo 'storing in db'
raster2pgsql -d -I -C -M -F -t 100x100 -s 4326 ${DATADIR}/built_lds/$YEAR/built_lds_${CITY}.tif raw.${CITY}_built_lds_${YEAR} > ${DATADIR}/built_lds/$YEAR/built_lds_${CITY}.sql

psql -d $PGDATABASE -h $PGHOST -U $POSTGRES_USER -f ${DATADIR}/built_lds/$YEAR/built_lds_${CITY}.sql
