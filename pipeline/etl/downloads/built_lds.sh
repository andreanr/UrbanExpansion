#!/bin/bash

CITY=$1
YEAR=$2

DATADIR=$3
LOCAL_FILE=$4

## descarga
wget -O $DATADIR/built_lds/$YEAR/$LOCAL_FILE http://cidportal.jrc.ec.europa.eu/ftp/jrc-opendata/GHSL/GHS_BUILT_LDSMT_GLOBE_R2015B/GHS_BUILT_LDS${YEAR}_GLOBE_R2016A_54009_250/V1-0/GHS_BUILT_LDS${YEAR}_GLOBE_R2016A_54009_250_v1_0.zip

echo 'unzip'
unzip -o -d $DATADIR/built_lds/$YEAR/ $DATADIR/built_lds/$YEAR/$LOCAL_FILE
