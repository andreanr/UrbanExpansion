import luigi
import pdb
from luigi import configuration
from luigi.contrib import postgres

from models.features import generate_features, generate_labels

from commons import city_task
from grids.grid_architect import GenerateUrbanGridsFeatures


class FeatureGenerator(city_task.FeaturesTask):
    """
    Task that calls feature table generation
    given the features specified on the
    experiment configuration
    """
    features = luigi.ListParameter()

    def requires(self):
        return GenerateUrbanGridsFeatures()

    @property
    def update_id(self):
        return ("""FeatureGenerator__{prefix}_{city}:{size}_{built}:{pop}:{cluster}:{dense_built}:{dense_pop}:{dense_cluster}"""
                .format(prefix=self.features_table_prefix,
                        city=self.city,
                        size=self.grid_size,
                        built=self.urban_built_threshold,
                        pop=self.urban_population_threshold,
                        cluster=self.urban_cluster_threshold,
                        dense_built=self.dense_built_threshold,
                        dense_pop=self.dense_population_threshold,
                        dense_cluster=self.dense_cluster_threshold))
    @property
    def table(self):
        return "features.{city}_{prefix}_{size}".format(city=self.city,
                                                        prefix=self.features_table_prefix,
                                                        size=self.grid_size)
    @property
    def query(self):
        return generate_features(self.city,
                                 self.features,
                                 self.features_table_prefix,
                                 self.grid_size,
                                 self.urban_built_threshold,
                                 self.urban_population_threshold,
                                 self.urban_cluster_threshold,
                                 self.dense_built_threshold,
                                 self.dense_population_threshold,
                                 self.dense_cluster_threshold)

class LabelGenerator(city_task.FeaturesTask):
    """
    Taks that generates urban labels based
    on the definition of urban by the built_lds threshold,
    population threshold and cluster threshold
    set on the luigi config file for the train and test years
    """
    def requires(self):
        return GenerateUrbanGridsFeatures()

    @property
    def update_id(self):
        return ("""LabelGenerator__{prefix}_{city}:{size}_{built}:{pop}:{cluster}"""
                 .format(prefix=self.labels_table_prefix,
                         city=self.city,
                         size=self.grid_size,
                         built=self.urban_built_threshold,
                         pop=self.urban_population_threshold,
                         cluster=self.urban_cluster_threshold))
    @property
    def table(self):
        return "features.{city}_{prefix}_{size}".format(city=self.city,
                                                        prefix=self.labels_table_prefix,
                                                        size=self.grid_size)
    @property
    def query(self):
        years = self.years_train + [ self.year_predict]
        years = [x for x in years if x]
        return generate_labels(self.city,
                               self.labels_table_prefix,
                               years,
                               self.grid_size,
                               self.urban_built_threshold,
                               self.urban_population_threshold,
                               self.urban_cluster_threshold)


