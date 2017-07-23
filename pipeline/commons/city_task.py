import luigi
import pdb
from luigi import configuration
from luigi.contrib import postgres


class CityGeneralTask(postgres.PostgresQuery):
    city = city = configuration.get('general','city')
    grid_size = configuration.get('general','grid_size')
    esri = configuration.get('general','esri')
    urban_built_threshold = configuration.get('general','urban_built_threshold')
    urban_population_threshold = configuration.get('general','urban_population_threshold')
    urban_cluster_threshold = configuration.get('general','urban_cluster_threshold')


class FeaturesTask(CityGeneralTask):
    year_train = configuration.get('general', 'year_train')
    year_test = configuration.get('general', 'year_test')
    year_predict = configuration.get('general', 'year_predict')
    features_table_prefix = configuration.get('general','features_table_prefix')
    labels_table_prefix = configuration.get('general','labels_table_prefix')
    model_comment = configuration.get('general','model_comment')
    dense_built_threshold = configuration.get('general','dense_built_threshold')
    dense_population_threshold = configuration.get('general','dense_population_threshold')
    dense_cluster_threshold = configuration.get('general','dense_cluster_threshold')
    features = luigi.Parameter()

