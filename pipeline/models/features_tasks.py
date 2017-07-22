import luigi
import pdb
from luigi import configuration
from luigi.contrib import postgres

from features import generate_features, generate_labels
## from commons import city_task

class FeatureGenerator(city_task.FeaturesTask):
    def requirements(self):
        return GridOrchestra()

    @property
    def query(self):
        return generate_features(self.city,
                                 self.features,
                                 self.features_table_name,
                                 self.grid_size,
                                 self.urban_built_threshold,
                                 self.urban_population_threshold,
                                 self.urban_cluster_threshold,
                                 self.dense_built_threshold,
                                 self.dense_population_threshold,
                                 self.dense_cluster_threshold)

class LabelGenerator(city_task.FeaturesTask):
    def requirements(self):
        return GridOrchestra()

    @property
    def query(self):
        years = [self.year_train, self.year_test, self.year_predict]
        years = [x for x in years if x]
        return generate_labels(self.city,
                               self.labels_table_name,
                               years,
                               self.grid_size,
                               self.built_threshold,
                               self.population_threshold,
                               self.cluster_threshold)


