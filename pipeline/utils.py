from sqlalchemy import create_engine
from sqlalchemy.pool import NullPool
from dotenv import load_dotenv, find_dotenv
import os
import yaml

# Variables de ambiente
load_dotenv(find_dotenv())


def get_engine():
    """
    Get SQLalchemy engine using credentials.
    Input stored in .env
    db: database name
    user: Username
    host: Hostname of the database server
    port: Port number
    passwd: Password for the database
    """
    database = os.environ.get("PGDATABASE")
    user = os.environ.get("POSTGRES_USER")
    password = os.environ.get("POSTGRES_PASSWORD")
    host = os.environ.get("PGHOST")
    port = os.environ.get("PGPORT")

    url = 'postgresql://{user}:{passwd}@{host}:{port}/{db}'.format(
        user=user, passwd=password, host=host, port=port, db=database)
    engine = create_engine(url)
    return engine


def get_features(experiment):
    return [x for x in experiment['Features'] if experiment['Features'][x]==True]


def read_yaml(config_file_name):
    """
    This function reads the config file
    Args:
       config_file_name (str): name of the config file
    """
    with open(config_file_name, 'r') as f:
        config = yaml.load(f)
    return config

def make_str(value):
    try:
        return unicode(value)
    except:
        return str(value)

def read_shapefile(filename='default.shp'):
    """
    read shapefile from specified directory and transform it
    into a geodataframe
    :param filename: str
    :return: geodataframe
    """

    gdf = gpd.from_file(filename)

    if not hasattr(gdf, 'gdf_name'):
        gdf.gdf_name = 'unnamed'

    return gdf

