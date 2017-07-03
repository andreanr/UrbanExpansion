#Script for downloading administrative boundaries for a
# given country and then quering OSM Overpass API

import geopandas as gpd
import geopandas_osm.osm
import os
import osmnx as ox
import subprocess
import argparse
import dotenv as de

# Variables de ambiente
de.load_dotenv(de.find_dotenv())


def run_command(cmd):
    """given shell command, returns communication tuple of stdout and stderr"""
    return subprocess.Popen(cmd,
                            stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE,
                            stdin=subprocess.PIPE).communicate()


def project_to_crs(filename, s_crs, t_crs, c_name):
    cmd = 'ogr2ogr -f "ESRI Shapefile"' + c_name + "_wgs84.shp " + "data/" + filename + "/" +\
          filename + " " + "-s_srs " + s_crs + " -t_srs " + t_crs
    exec_cmd = run_command(cmd)
    print(exec_cmd)


def get_highways(geodf):
    city_boundary = geodf.ix[0].geometry
    df = geopandas_osm.osm.query_osm('way', city_boundary, recurse='down', tags='highway')
    # Delimitamos a solo lineas
    df = df[df.type == 'LineString'][['highway', 'name', 'geometry']]


def shp_to_pg(path):
    database = os.environ.get("PGDATABASE")
    user = os.environ.get("POSTGRES_USER")
    # password = os.environ.get("POSTGRES_PASSWORD")
    host = os.environ.get("PGHOST")
    # port = os.environ.get("PGPORT")
    cmd = 'shp2pgsql -s 4326 -d -D -I -W "latin1"' + path + " | psql -d " + database + ' -h ' +\
          host + ' -U ' + user
    exec_cmd = run_command(cmd)
    print(exec_cmd)


if __name__ == "__main__":
    ox.config(log_file=True, log_console=True, use_cache=True)
    parser = argparse.ArgumentParser()
    parser.add_argument("--country", type=str, help="pass your country name", default="jordan")
    parser.add_argument("--city", type=str, help="pass your city name", default="amman")
    parser.add_argument("--buffer", type=str, help="pass boundary buffer", default="25000")
    parser.add_argument("--pgdatabase", type=str, help="pass database name", default="25000")
    args = parser.parse_args()
    country_name = args.country
    city_name = args.city
    buff_dist = int(args.buffer)
    query = {'city': city_name}
    city = ox.gdf_from_place(query, buffer_dist=buff_dist)
    fn = city_name + ".shp"
    ox.save_gdf_shapefile(city)
    city_boundary = city.ix[0].geometry
    # df = geopandas_osm.osm.query_osm('way', city_boundary, recurse='down', tags='highway')
    # print(type(df))
    # df = df[df.type == 'LineString'][['highway', 'name', 'geometry']]
    # df.plot()
    # city = ox.project_gdf(city)
    # fig, ax = ox.plot_shape(city)
    from_crs = "EPSG:3857"
    to_crs = "EPSG:4326"
    # project_to_crs(filename=fn, folder="shp", s_crs=from_crs, t_crs=to_crs, c_name=country_name)
    path = "data/" + city_name + "/" + fn
    shp_to_pg(path)
