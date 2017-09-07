# Script for inserting into db the shapefiles of
# water bodies

import geopandas as gpd
import subprocess
import argparse
import fiona
import pdb
import os

from shapely.geometry import Point, Polygon, shape, LineString
from progress.bar import Bar


def run_command(cmd):
    """given shell command, returns communication tuple of stdout and stderr"""
    return subprocess.call(cmd, shell=True)


def polygon_from_coordinates(cc):
    coordinates = cc["coordinates"][0]
    points = []
    for c in coordinates:
        points.append(Point(c))
    poly = Polygon([[p.x, p.y] for p in points])

    return poly


def crop_shp(source_shp, destination_shp, buffer_shp):
    buff = fiona.open(buffer_shp)
    pol = buff[0]
    db = shape(pol["geometry"])
    water = fiona.open(source_shp)
    mm = len(water)
    data = []
    bar = Bar('Processing', max=mm, suffix='%(index)d/%(max)d - %(percent).1f%% - %(eta)ds')
    for w in water:
        pol = w["geometry"]
        lista = pol['coordinates'][0]
        if len(lista) >= 3:
            try:
                ppp = []
                if isinstance(lista[0], list):
                    for l in lista:
                        ppp.append(Polygon(l))
                else:
                    ppp.append(Polygon(lista))
            except AssertionError as ex:
                print("Unexpected error with polygon: {error}".format(error=ex))
            for p in ppp:
                dw = db.intersection(p)
                #dbp = gpd.GeoSeries(p)
                #df1 = gpd.GeoDataFrame({'geometry': dbp})
                #pp = gpd.GeoSeries(p)
                #df2 = gpd.GeoDataFrame({'geometry':pp})
                #dw = gpd.overlay(df1, df2, how='intersection')
                #if not dw.empty:
                #    if isinstance(dw["geometry"][0], Polygon):
                #        data.append(dw["geometry"][0])
                if not dw.is_empty:
                    if isinstance(dw, Polygon):
                        data.append(dw)
        bar.next()
    bar.finish()
    
    df = gpd.GeoDataFrame()
    if len(data) > 0:
        df['geometry'] = data
        df.crs = buff.crs
        df.to_file(destination_shp, driver='ESRI Shapefile')
    else:
        open(destination_shp, 'a').close()


def shp_to_pg(path, city_name, local):
    print('Saving query for upload to db')
    if os.stat(path).st_size == 0:
        query = "create table raw." + city_name + "_water_bodies(" + \
                "gid int4," + \
	        "fid float8," + \
	        "geom geometry);"
        with open(local + "/water_bodies/water_bodies_" + city_name + '.sql', "w") as q:
            q.write(query)
    else:
        cmd = 'shp2pgsql -s 4326 -c -W "latin1" ' + \
              path + " raw." + city_name + "_water_bodies" + " > " + \
              local + "/water_bodies/water_bodies_" + city_name + '.sql'
        exec_cmd = run_command(cmd)
        print(exec_cmd)

    msg = "Query saved at: {pth}".format(pth=local_path + '/water_bodies/water_bodies_' + city_name + '.sql')
    print(msg)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--city", type=str, help="pass your city name", default="mafraq")
    parser.add_argument("--local_path", type=str, help="path to save local downloads", default="/home/data")
    args = parser.parse_args()
    city = args.city
    local_path = args.local_path
    b_shp = local_path + '/shp_buffer/' + city + '.shp'
    s_shp = local_path + '/water_bodies/water_bodies.shp'
    d_shp = local_path + "/water_bodies/water_bodies_" + city + ".shp" 
    crop_shp(source_shp=s_shp, destination_shp=d_shp, buffer_shp=b_shp)
    shp_to_pg(d_shp, city, local_path)

