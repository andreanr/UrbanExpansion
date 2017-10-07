import luigi
import pdb
import json
from luigi import configuration
from luigi.contrib import postgres

import os
from dotenv import find_dotenv, load_dotenv

load_dotenv(find_dotenv())


class PostgresTask(postgres.PostgresQuery):
    # RDS
    database = os.environ.get("PGDATABASE")
    user = os.environ.get("POSTGRES_USER")
    password = os.environ.get("POSTGRES_PASSWORD")
    host = os.environ.get("PGHOST")


class CityGeneralTask(PostgresTask):
    city = configuration.get_config().get('general','city')
    grid_size = configuration.get_config().get('general','grid_size')
    esri = configuration.get_config().get(city,'esri')
    urban_built_threshold = configuration.get_config().get('general','urban_built_threshold')
    urban_population_threshold = configuration.get_config().get('general','urban_population_threshold')
    urban_cluster_threshold = configuration.get_config().get('general','urban_cluster_threshold')


class FeaturesTask(CityGeneralTask):
    years_train = json.loads(configuration.get_config().get('general', 'years_train'))
    year_test = configuration.get_config().get('general', 'year_test')
    label_range = json.loads(configuration.get_config().get('general', 'label_range'))
    features_table_prefix = configuration.get_config().get('general','features_table_prefix')
    labels_table_prefix = configuration.get_config().get('general','labels_table_prefix')
    model_comment = configuration.get_config().get('general','model_comment')
    dense_built_threshold = configuration.get_config().get('general','dense_built_threshold')
    dense_population_threshold = configuration.get_config().get('general','dense_population_threshold')
    dense_cluster_threshold = configuration.get_config().get('general','dense_cluster_threshold')
