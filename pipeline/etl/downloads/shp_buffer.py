# Script for downloading administrative boundaries for a
# given country and then quering OSM Overpass API

import geopandas as gpd
import geopandas_osm.osm
import osmnx as ox
import subprocess
import argparse
import dotenv as de
import os
import json
import pdb

# Variables de ambiente
de.load_dotenv(de.find_dotenv())


def make_str(value):
    try:
        return unicode(value)
    except:
        return str(value)


def save_shapefile(gdf, folder=None, filename='default.shp'):
    folder_path = '{}/{}'.format(folder, filename)

    # make everything but geometry column a string
    for col in [c for c in gdf.columns if not c == 'geometry']:
        gdf[col] = gdf[col].fillna('').map(make_str)

    gdf.to_file(folder_path)

    if not hasattr(gdf, 'gdf_name'):
        gdf.gdf_name = 'unnamed'


def run_command(cmd):
    """given shell command, returns communication tuple of stdout and stderr"""
    return subprocess.call(cmd, shell=True)


def project_to_crs(filename, s_crs, t_crs, c_name):
    cmd = 'ogr2ogr -f "ESRI Shapefile"' + c_name + "_wgs84.shp " + "data/" + filename + "/" +\
          filename + " " + "-s_srs " + s_crs + " -t_srs " + t_crs
    exec_cmd = run_command(cmd)
    print(exec_cmd)


def shp_to_pg(path, city_name):
    database = os.environ.get("PGDATABASE")
    user = os.environ.get("POSTGRES_USER")
    password = os.environ.get("POSTGRES_PASSWORD")
    host = os.environ.get("PGHOST")
    # port = os.environ.get("PGPORT")
    hosts = "postgresql://{user}:{password}@{host}/{database}"
    hosts = hosts.format(user=user, password=password, host=host, database=database)
    cmd = 'shp2pgsql -s 4326 -d -D -I -W "latin1" ' + path + " raw." + city_name +\
          " | psql " + hosts 

    exec_cmd = run_command(cmd)
    print(exec_cmd)


if __name__ == "__main__":
    ox.config(log_file=True, log_console=True, use_cache=True)
    parser = argparse.ArgumentParser()
    parser.add_argument("--city", type=str, help="pass your city name", default="amman")
    parser.add_argument("--buffer", type=str, help="pass boundary buffer", default="25000")
    parser.add_argument("--local_path", type=str, help="path to local download", default="/home/data/shp_buffer")
    parser.add_argument("--data_task", type=str, help="shp_buffer", default="shp_buffer")
    args = parser.parse_args()
    city_name = args.city
    local_path = args.local_path
    data_task = args.data_task
    buff_dist = int(args.buffer)
    query = {'city': city_name}
    city = ox.gdf_from_place(query, buffer_dist=buff_dist)
    labs = ['bbox_west', 'bbox_south', 'bbox_east', 'bbox_north']
    vals = city.total_bounds
    bbox = dict(zip(labs,vals))
    path = local_path + '/' + data_task + '/' + city_name + '_' + data_task + ".json"
    with open(path, 'w') as fp:
        json.dump(bbox, fp)
    fn = city_name + ".shp"
    save_shapefile(city, folder=local_path + '/' + data_task, filename=fn)
    shp_to_pg(path, city_name)


