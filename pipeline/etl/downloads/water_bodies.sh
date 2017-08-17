#!/bin/bash

CITY=$1

DATADIR=$2
LOCAL_FILE=$3

# Download from: https://www.worldwildlife.org/publications/global-lakes-and-wetlands-database-small-lake-polygons-level-2
curl https://c402277.ssl.cf1.rackcdn.com/publications/17/files/original/GLWD-level2.zip?1343838637 --create-dirs -o $DATADIR/water_bodies/$LOCAL_FILE

echo 'unzip'
unzip -o -d $DATADIR/water_bodies/ $DATADIR/water_bodies/$LOCAL_FILE

echo "EOGCS["GCS_WGS_1984",DATUM["D_WGS_1984",SPHEROID["WGS_1984",6378137,298.257223563]],PRIMEM["Greenwich",0],UNIT["Degree",0.017453292519943295]]" > ${DATADIR}/water_bodies/glwd_2.prj
