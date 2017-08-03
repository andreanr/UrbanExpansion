#!/bin/bash

CITY=$1
YEAR=$2

DATADIR=$3
LOCAL_FILE=$4

# Get links for download
cat ${DATADIR}/city_lights/ligas_lights.csv | grep $YEAR | awk -F ',' '{print $2}' > $DATADIR/city_lights/$YEAR/temp.txt

## Download data for the appropriate year
for i in $(cat ${DATADIR}/city_lights/$YEAR/temp.txt); 
	do curl $i --create-dirs -o $DATADIR/city_lights/$YEAR/$LOCAL_FILE
	break 1
done

# Extract
for z in $DATADIR/city_lights/$YEAR/*.tgz; do tar -xvzf $z -C $DATADIR/city_lights/$YEAR/ ; done
