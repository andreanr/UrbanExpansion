#!/bin/bash

source ../../.env

YEAR=2005
mkdir /$YEAR
cd $YEAR/

# Get links for download
cat ligas_lights.csv | grep $YEAR | awk -F ',' '{print $2}' > temp.txt

# Download data for the appropriate year
for i in $(cat temp.txt); do wget $i done

# Extract 
for z in *.tgz; do tar -xvzf $z; done 

# Remove temp file
rm temp.txt

# Cut raster to shp and insert raster to db
for z in *.tif; do 
	gdalwarp -cutline ${countries_shp}/${COUNTRY}.shp -crop_to_cutline -dstalpha $z ../$YEAR/$z{_cut.tif};
	raster2pgsql -d -I -C -M -F -t 150x150 -s 32613 $z > lights.sql;
	psql -d $DB_NAME -h $DB_HOST -U $DB_USER -f lights.sql
done

