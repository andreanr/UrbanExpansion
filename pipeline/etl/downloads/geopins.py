# Script for downloading administrative boundaries for a
# given country and then quering OSM Overpass API

import pdb
import requests
import argparse
import json
import fiona
import pandas as pd
import geopandas as gpd
import time
import logging
import warnings

from progress.bar import Bar  # sudo pip install progress
from pandas.io.common import urlencode
from shapely.geometry import Point

_crs = fiona.crs.from_epsg(4326)
warnings.filterwarnings("ignore")


def fix_column_names(df):
    aux = []
    sec = 1
    for x in df.columns:
        try:
            x = x.lower()
            x = x.replace(':', '_')
            col = x.encode('latin-1')
            col = col.decode('latin-1')
        except Exception as e:
            # logging.error('Failed.', exc_info=e)
            col = 'undef_{isec}'.format(isec=sec)
            sec += 1
        finally:
            aux.append(col)
    formato = {'addr_country': 'country', 'addr_housename': 'housename', 'addr_housenumber':'housenum',
               'addr_postcode':'postcode', 'addr_street':'street', 'alt_name_en':'name_en',
               'artwork_type':'artwork', 'building_levels':'buildlevel', 'contact_fax':'fax',
               'contact_phone':'phone', 'contact_website':'website', 'denomination':'denom',
               'description':'descrip', 'description_payment':'payment', 'designation':'design',
               'diet_vegetarian':'vegetarian', 'drive_through':'drive_thru', 'internet_access':'web_access',
               'internet_access_fee':'web_fee', 'microbrewery':'micrbrewry', 'opening_hours':'open_hrs',
               'osak_municipality_name':'osak_munic', 'outdoor_seating':'outseating', 'social_facility_for':'social',
               'toilets_wheelchair':'wheelchair','tower_construction':'towerconst','wheelchair_description':'wheeldesc',
               'communication_antenna':'commantena'}
    auxx = []
    for a in aux:
        if a in formato.keys():
            auxx.append(formato[a])
        else:
            auxx.append(a)
    df.columns = auxx

    return df


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

    for tt in element['tags']:
        node[tt] = element['tags'][tt]

    return node


def get_data_points(bbox_city):
    """
    :param bbox_city: diccionario del bounding box
    :return: GeoDataFrame con highways
    """

    print("Retrieving data from the OSM Overpass API...")
    filtros = ['["amenity"]', '["landuse"]', '["building"]', '["leisure"]', '["natural"]',
               '["shop"]', '["historic"]', '["tourism"]', '["man_made"]', '["bicycle_parking"]',
               '["cuisine"]', '["power"]', '["office"]','["aeroway"]']
    df = gpd.GeoDataFrame([])
    mm = len(filtros)
    bar = Bar('Processing', max=mm, suffix='%(index)d/%(max)d - %(percent).1f%% - %(eta)ds')
    for filtro in filtros:
        url = define_url_node(bbox_city=bbox_city, filtro=filtro)
        response = requests.get(url)
        response_json = response.json()
        nodos = dict()
        for element in response_json['elements']:
            if element['type'] == 'node':
                key = element['id']
                nodos[key] = get_node(element)
        if len(nodos) > 0:
            df_nodos = pd.DataFrame.from_dict(nodos)
            df_nodos = df_nodos.transpose()
            df_nodos = fix_column_names(df_nodos)
            df_nodos = df_nodos.loc[:, ~df_nodos.columns.duplicated()]
            df = pd.concat([df,df_nodos], axis=0, ignore_index=True)
        bar.next()
    bar.finish()
    points = [Point(x['lon'], x['lat']) for i, x in df.iterrows()]
    df = df.drop(['lon', 'lat'], axis=1)
    if not ("aeroway" in df.columns):
        df["aeroway"] = None
    df = df.set_geometry(points, crs=_crs)

    return df


def define_url_node(bbox_city, filtro, recurse='down', meta=True):
    """
    url for Overpass API request
    :param bbox_city: diccionario con bounding box
    :param filtro: filtro para el query
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

    query_template = '[out:json][timeout:{timeout}];(node{filters}({south:.8f},' + \
                     '{west:.8f},{north:.8f},{east:.8f});{recurse};);out {meta};'

    query_str = query_template.format(north=north, south=south, east=east, west=west, filters=filtro,
                                      timeout=timeout, recurse=recursestr, meta=metastr)
    url = 'http://www.overpass-api.de/api/interpreter?'
    prepared_url = ''.join([url, urlencode({'data': query_str})])

    return prepared_url


if __name__ == "__main__":
    logging.basicConfig()
    parser = argparse.ArgumentParser()
    parser.add_argument("--city", type=str, help="pass your city name", default="semarang")
    parser.add_argument("--timeout", type=float, help="specify timeout for API request", default=180)
    parser.add_argument("--local_path", type=str, help="local path to file download", default="/home/data")
    args = parser.parse_args()
    city_name = args.city
    timeout = args.timeout
    local_path = args.local_path
    pth = local_path + "/shp_buffer/" + city_name + "_shp_buffer.json"
    bbox_file = open(pth)
    bbox = json.load(bbox_file)
    start = time.time()
    nodes_points = get_data_points(bbox)
    pth = local_path + "/geopins/" + city_name + "_geopins.shp"
    nodes_points.to_file(pth)
    end = time.time()
    t = end - start
    m, s = divmod(t, 60)
    h, m = divmod(m, 60)
    h = int(round(h, 0))
    m = int(round(m, 0))
    s = round(s, 2)
    time_str = 'Download geopins  and converted to shapefile in {hr}:{min}:{seg}.'.format(hr=h, min=m, seg=s)
    print(time_str)
    print("Finished processing. Shapefiles saved at: " + pth)
