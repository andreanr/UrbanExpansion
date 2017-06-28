#!/bin/bash

mkdir shapefiles
cd ./shapefiles/

# amman
mkdir amman_jordan
cd ./amman_jordan
wget https://s3.amazonaws.com/metro-extracts.mapzen.com/amman_jordan.osm2pgsql-shapefiles.zip
unzip amman_jordan.osm2pgsql-shapefiles.zip
wget https://s3.amazonaws.com/metro-extracts.mapzen.com/amman_jordan.osm2pgsql-geojson.zip
unzip amman_jordan.osm2pgsql-geojson.zip
rm https://s3.amazonaws.com/metro-extracts.mapzen.com/amman_jordan.osm2pgsql-shapefiles.zip
rm https://s3.amazonaws.com/metro-extracts.mapzen.com/amman_jordan.osm2pgsql-geojson.zip
