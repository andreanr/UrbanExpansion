#!/bin/bash



DATADIR='/Users/andreanavarrete/workspace/UrbanExpansion/data/'
LOCAL_FILE='amman_dem.tif'


# Download
function jsonValue() {
KEY=$1
num=$2
awk -F"[,:}]" '{for(i=1;i<=NF;i++){if($i~/'$KEY'\042/){print $(i+1)}}}' | tr -d '"' | sed -n ${num}p
}


SOUTH=$(cat /Users/andreanavarrete/workspace/UrbanExpansion/data/bbox.json | jsonValue bbox_south)
WEST=$(cat /Users/andreanavarrete/workspace/UrbanExpansion/data/bbox.json | jsonValue bbox_west)
EAST=$(cat /Users/andreanavarrete/workspace/UrbanExpansion/data/bbox.json | jsonValue bbox_east)
NORTH=$(cat /Users/andreanavarrete/workspace/UrbanExpansion/data/bbox.json | jsonValue bbox_north)

eio --product SRTM3 clip -o $DATADIR/$LOCAL_FILE --bounds $WEST $SOUTH $EAST $NORTH


