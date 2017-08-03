#!/bin/bash

CITY=$1
YEAR=$2

DATADIR=$3
LOCAL_FILE=$4

## descarga
curl http://cidportal.jrc.ec.europa.eu/ftp/jrc-opendata/GHSL/GHS_SMOD_POP_GLOBE_R2016A/GHS_SMOD_POP${YEAR}_GLOBE_R2016A_54009_1k/V1-0/GHS_SMOD_POP${YEAR}_GLOBE_R2016A_54009_1k_v1_0.zip \
     --create-dirs -o $DATADIR/settlements/$YEAR/$LOCAL_FILE

echo 'unzip'
unzip -o -d $DATADIR/settlements/$YEAR/ $DATADIR/settlements/$YEAR/$LOCAL_FILE
