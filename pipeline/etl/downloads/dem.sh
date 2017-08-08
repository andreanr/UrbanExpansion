#!/bin/bash

DATADIR=$1
DATATASK=$2
CITY=$3
LOCAL_FILE=$4

echo $DATADIR
echo $LOCAL_FILE

# Download
jsonValue () {
KEY=$1
num=$2
awk -F"[,:}]" '{for(i=1;i<=NF;i++){if($i~/'$KEY'\042/){print $(i+1)}}}' | tr -d '"' | sed -n ${num}p
}


SOUTH=$(cat $DATADIR/shp_buffer/shp_buffer_${CITY}.json | jsonValue bbox_south)
WEST=$(cat $DATADIR/shp_buffer/shp_buffer_${CITY}.json | jsonValue bbox_west)
EAST=$(cat $DATADIR/shp_buffer/shp_buffer_${CITY}.json | jsonValue bbox_east)
NORTH=$(cat $DATADIR/shp_buffer/shp_buffer_${CITY}.json | jsonValue bbox_north)

echo $NORTH
echo $WEST
echo $EAST
echo $SOUTH

eio --product SRTM3 clip -o $DATADIR/${DATATASK}/$LOCAL_FILE --bounds $WEST $SOUTH $EAST $NORTH


