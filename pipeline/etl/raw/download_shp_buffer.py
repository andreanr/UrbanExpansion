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
    # password = os.environ.get("POSTGRES_PASSWORD")
    host = os.environ.get("PGHOST")
    # port = os.environ.get("PGPORT")
    cmd = 'shp2pgsql -s 4326 -d -D -I -W "latin1" ' + path + " raw." + city_name  +\
          " | psql -d " + database + ' -h ' +\
          host + ' -U ' + user
    exec_cmd = run_command(cmd)
    print(exec_cmd)


if __name__ == "__main__":
    ox.config(log_file=True, log_console=True, use_cache=True)
    parser = argparse.ArgumentParser()
    parser.add_argument("--country", type=str, help="pass your country name", default="jordan")
    parser.add_argument("--city", type=str, help="pass your city name", default="amman")
    parser.add_argument("--buffer", type=str, help="pass boundary buffer", default="25000")
    args = parser.parse_args()
    country_name = args.country
    city_name = args.city
    buff_dist = int(args.buffer)
    query = {'city': city_name}
    city = ox.gdf_from_place(query, buffer_dist=buff_dist)
    labs = ['bbox_west', 'bbox_south', 'bbox_east', 'bbox_north']
    vals = city.total_bounds
    bbox = dict(zip(labs,vals))
    path = "/home/data/boundries/" + city_name + "_bbox.json"
    with open(path, 'w') as fp:
        json.dump(bbox, fp)
    fn = city_name + ".shp"
    save_shapefile(city, folder='/home/data/boundries', filename=fn)
    city_boundary = city.ix[0].geometry
    # project_to_crs(filename=fn, folder="shp", s_crs=from_crs, t_crs=to_crs, c_name=country_name)
    path = "/home/data/boundries/" + fn
    shp_to_pg(path, city_name)

