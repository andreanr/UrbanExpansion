import luigi
import pdb
from luigi import configuration
from luigi.contrib import postgres

from models.features import generate_features, generate_labels

from commons import city_task
from grids.grid_architect import GenerateUrbanGridsFeatures


class FeatureGenerator(city_task.FeaturesTask):
    features = luigi.ListParameter()

    def requires(self):
        return GenerateUrbanGridsFeatures()

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
 
    def requires(self):
        return GenerateUrbanGridsFeatures()

    @property
    def table(self):
        return "features.{city}_{prefix}_{size}".format(city=self.city,
                                                        prefix=self.labels_table_prefix,
                                                        size=self.grid_size)
    @property
    def query(self):
        years = [self.year_train, self.year_test, self.year_predict]
        years = [x for x in years if x]
        return generate_labels(self.city,
                               self.labels_table_prefix,
                               years,
                               self.grid_size,
                               self.urban_built_threshold,
                               self.urban_population_threshold,
                               self.urban_cluster_threshold)


