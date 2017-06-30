#!/bin/bash

. ./../../.env

YEAR=2005
CITY='amman'
COUNTRY='Jordan'


#mkdir ${DATADIR}/city_lights
#mkdir ${DATADIR}/city_lights/$YEAR

cd ${DATADIR}/city_lights/$YEAR

# Get links for download
#cat ${DATADIR}/city_lights/ligas_lights.csv | grep $YEAR | awk -F ',' '{print $2}' > temp.txt

## Download data for the appropriate year
#for i in $(cat temp.txt); do wget $i; done

# Extract
#for z in *.tgz; do tar -xvzf $z; done 

# Remove temp file
#rm temp.txt

# Cut raster to shp
for z in *.tif; do 
	gdalwarp -cutline ${DATADIR}/boundries/${COUNTRY}.shp -crop_to_cutline -dstalpha $z ${DATADIR}/city_lights/$YEAR/${z}_${CITY}.tif
	break 1
done

# upload raster to db
raster2pgsql -d -I -C -M -F -t 100x100 -s 4326 ${DATADIR}/city_lights/$YEAR/*_${CITY}.tif raw.${CITY}_city_lights_${YEAR} > ${DATADIR}/city_lights/$YEAR/lights_${CITY}.sql
psql -d $PGDATABASE -h $PGHOST -U $POSTGRES_USER -f ${DATADIR}/city_lights/$YEAR/lights_${CITY}.sql
