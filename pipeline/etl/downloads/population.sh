#!/bin/bash

CITY=$1
YEAR=$2

DATADIR=$3
LOCAL_FILE=$4

# descarga
wget -O $DATADIR/population/$YEAR/$LOCAL_FILE http://cidportal.jrc.ec.europa.eu/ftp/jrc-opendata/GHSL/GHS_POP_GPW4_GLOBE_R2015A/GHS_POP_GPW4${YEAR}_GLOBE_R2015A_54009_250/V1-0/GHS_POP_GPW4${YEAR}_GLOBE_R2015A_54009_250_v1_0.zip

#unzip files into directory
echo 'unzip'
unzip -o -d $DATADIR/population/$YEAR/ $DATADIR/population/$YEAR/$LOCAL_FILE
