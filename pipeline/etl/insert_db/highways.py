# Script for inserting into db the shapefiles of
# highways for a given city previously obtained
# from de OSM Overpass API

import geopandas as gpd
import subprocess
import argparse
import dotenv as de
import os
import pdb


# Variables de ambiente
de.load_dotenv(de.find_dotenv())


def run_command(cmd):
    """given shell command, returns communication tuple of stdout and stderr"""
    return subprocess.call(cmd, shell=True)


def shp_to_pg(path, city_name, local_path):
    database = os.environ.get("PGDATABASE")
    user = os.environ.get("POSTGRES_USER")
    password = os.environ.get("POSTGRES_PASSWORD")
    host = os.environ.get("PGHOST")
    # port = os.environ.get("PGPORT")
    cmd = 'export PGPASSWORD="' + password + '"; ' + 'shp2pgsql -s 4326 -d -D -I -W "latin1" ' +\
          path + " raw." + city_name + "_highways" + " > " + local_path + '/highways/ + 
          'highways_' + city_name + '.sql'
    exec_cmd = run_command(cmd)
    print(exec_cmd)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--city", type=str, help="pass your city name", default="amman")
    parser.add_argument("--local_path", type=str, help="path to save local downloads", default="/home/data")
    args = parser.parse_args()
    city = args.city
    local_path = args.city
    pth = local_path + "/highways/" + city + "_highways.shp"
    shp_to_pg(pth, city, local_path)

