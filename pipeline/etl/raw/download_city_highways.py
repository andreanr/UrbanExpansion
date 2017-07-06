# Script for downloading highways inside a given bounding
# box for a given country by quering the OSM Overpass API

import requests
import argparse
import json
import fiona
import pandas as pd
import geopandas as gpd
import timeit
# import pdb
# import pickle

from progress.bar import Bar # sudo pip install progress
from pandas.io.common import urlencode
from shapely.geometry import mapping, Point, Polygon, LineString
from itertools import groupby

_crs = fiona.crs.from_epsg(4326)
_bbox = dict()

def get_node(element):
    """
    :param element: dict representing node element
    :return: dict with relevant info for element
    """
    node = dict()
    node['lat'] = element['lat']
    node['lon'] = element['lon']
    node['osmid'] = element['id']

    node['timestamp'] = element['timestamp']
    return node


def get_path(element):
    """
    :param element: dict representing node element
    :return: dict with relevant info for element
    """
    path = dict()
    path['osmid'] = element['id']

    # remove any consecutive duplicate elements in the list of nodes
    grouped_list = groupby(element['nodes'])
    path['nodes'] = [group[0] for group in grouped_list]

    if 'tags' in element:
        for tag in element['tags']:
            path[tag] = element['tags'][tag]

    path['timestamp'] = element['timestamp']

    return path


def get_highways(bbox_city):
    """
    :return: GeoDataFrame con highways
    """

    url = define_url(bbox_city=bbox_city)
    # pdb.set_trace()
    response = requests.get(url)
    # pickle.dump(response, open("response.p", "wb"))
    # response = pickle.load(open("response.p", "rb"))
    response_json = response.json()
    print("Retrieving data from the OSM Overpass API...")
    nodes = dict()
    paths = dict()
    bar = Bar('Processing', max=20, suffix='%(index)d/%(max)d - %(percent).1f%% - %(eta)ds')
    for element in response_json['elements']:
        if element['type'] == 'node':
            key = element['id']
            nodes[key] = get_node(element)
        elif element['type'] == 'way':  # osm calls network paths 'ways'
            key = element['id']
            paths[key] = get_path(element)
        bar.next()
    bar.finish()
    # pickle.dump(nodes, open("nodes.p", "wb"))
    # pickle.dump(paths, open("paths.p", "wb"))
    # nodes = pickle.load(open("nodes.p", "rb"))
    # paths = pickle.load(open("paths.p", "rb"))
    df_nodes = pd.DataFrame.from_dict(nodes)
    df_nodes = df_nodes.transpose()
    df_nodes['timestamp'] = pd.to_datetime(df_nodes['timestamp'])
    # pdb.set_trace()
    nodes = df_nodes.dropna(subset=df_nodes.columns.drop(['osmid', 'lon', 'lat']), how='all')
    points = [Point(x['lon'], x['lat']) for i, x in nodes.iterrows()]
    nodes = nodes.drop(['lon', 'lat'], axis=1)
    nodes = nodes.set_geometry(points, crs=_crs)

    def wayline(path_way):
        lista_nodes = path_way["nodes"]
        nodes_way = nodes.ix[lista_nodes]
        nodes_way = nodes_way["geometry"]
        lista_nodos = nodes_way.tolist()
        return LineString(lista_nodos)

    # pdb.set_trace()
    print("Proceed to LineString transformation for each highway.")
    lineas = [wayline(paths[path]) for path in paths.keys()]

    # pickle.dump(lineas, open("lineas.p", "wb"))
    # lineas = pickle.load(open("lineas.p", "rb"))

    print("Constructing geodataframe of highways.")
    df_paths = pd.DataFrame.from_dict(paths)
    df_paths = df_paths.transpose()
    df_paths = df_paths[["osmid", "name", "aeroway", "bridge", "capacity", "highway", "religion", "tourism", "timestamp"]]
    gdf_paths = gpd.GeoDataFrame(df_paths)
    gdf_paths['geometry'] = lineas
    gdf_paths.crs = _crs

    return nodes, gdf_paths


def define_url(bbox_city, recurse='down', meta=True):
    """
    url for Overpass API request
    :param bbox_city: diccionario con bounding box
    :param recurse: 'up','down','uprel','downrel' de acuerdo a la API
    :param meta: necesita ser True
    :return: url para la peticion
    """
    if meta:
        metastr = 'meta'
    else:
        metastr = ''
    recurse_map = {
        'up': '<',
        'uprel': '<<',
        'down': '>',
        'downrel': '>>',
    }
    recursestr = recurse_map[recurse]
    west = float(bbox_city["bbox_west"])
    south = float(bbox_city["bbox_south"])
    east = float(bbox_city["bbox_east"])
    north = float(bbox_city["bbox_north"])
    # turn bbox into a polygon
    polygon = Polygon([(west, south), (east, south), (east, north), (west, north)])
    polygon.crs = _crs
    ppth = "/home/data/boundries/" + city_name + "_bbox.shp"
    # Define a polygon feature geometry with one attribute
    schema = {
        'geometry': 'Polygon',
        'properties': {'id': 'int'},
    }
    # write the shp
    with fiona.open(ppth, 'w', 'ESRI Shapefile', schema) as c:
        c.write({
            'geometry': mapping(polygon),
            'properties': {'id': 1},
        })

    query_template = '[out:json][timeout:{timeout}];(way{filters}({south:.8f},' + \
                     '{west:.8f},{north:.8f},{east:.8f});{recurse};);out {meta};'
    print(polygon)
    filtro = '["area"!~"yes"]["highway"!~"proposed|construction|abandoned|platform|raceway"]'
    query_str = query_template.format(north=north, south=south, east=east, west=west, filters=filtro,
                                      timeout=timeout, recurse=recursestr, meta=metastr)
    url = 'http://www.overpass-api.de/api/interpreter?'
    prepared_url = ''.join([url, urlencode({'data': query_str})])

    return prepared_url


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--city", type=str, help="pass your city name", default="amman")
    parser.add_argument("--timeout", type=float, help="specify timeout for API request", default=180)
    args = parser.parse_args()
    city_name = args.city
    timeout = args.timeout
    pth = "/home/data/boundries/" + city_name + "_bbox.json"
    bbox_file = open(pth)
    bbox = json.load(bbox_file)
    start = timeit.timeit()
    nodes_highways, paths_highways = get_highways(bbox)
    end = timeit.timeit()
    t = end - start
    pth = "/home/data/highways/" + city_name + "_highways.shp"
    print("Download highways and converted to shapefile in " + t + " seconds")
    paths_highways.to_file(pth)

