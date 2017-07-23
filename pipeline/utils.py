from sqlalchemy import create_engine
from sqlalchemy.pool import NullPool
from dotenv import load_dotenv, find_dotenv
from luigi import configuration
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

def nonurban_grids(grid_tables_path):
    grid_tables_dict = read_yaml(grid_tables_path)
    nonurban = [grid for grid in grid_tables_dict.keys()
                if 'urban' not in grid]
    return nonurban


def urban_grids(grid_tables_path):
    grid_tables_dict = read_yaml(grid_tables_path)
    urban = [grid for grid in grid_tables_dict.keys()
                if 'urban' in grid]
    return urban


def get_features_years(feature_grid):
    try:
        years_data = configuration.get_config().get(feature_grid, 'years')
        years_data = [x.strip() for x in years_data.split(',')]
        years = dict()
        for year in years_data:
            year_model = configuration.get_config().get(feature_grid, year)
            years[year_model] = year
        return years
    except:
        return dict()


def get_years_models():
    return [configuration.get_config().get('general', 'year_train'),
            configuration.get_config().get('general', 'year_test'),
            configuration.get_config().get('general', 'year_predict')]


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

