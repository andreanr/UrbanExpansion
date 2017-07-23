import datetime
import logging
import luigi
import psycopg2
import re
import tempfile

from grids import *

class FeatureGrid(CityGeneralTask):
    grid_tables_path = luigi.Parameter()
    feature = luigi.Parameter()
    year_model = luigi.Parameter()
    year_data = luigi.Parameter()

    def requires(self):
       return [GenerateGridTables(self.city, self.grid_size, self.grid_tables_path),
               AddPrimaryKey(self.city, self.esri, self.grid_size)]

    @property
    def query(self):
        feature_function = eval(self.feature)
        return feature_function(grid_size,
                                city,
                                esri,
                                year_data,
                                year_model)


class GenerateFeaturesGrids(luigi.Wrapper):
    grid_tables_path = configuration.get('general','grid_tables_path')

    def requires(self):
        ## TODO
        non_urbanfeatures = utils.nonurban_grids(self.grid_tables_path)
        feature_tasks = []
        for feature_grid in non_urbanfeatures:
            ## TODO
            years_dict = utils.get_features_years(feature_grid)
            for year_model, year_data in years_dict.items():
                feature_tasks.append(FeatureGrid(self.grid_tables_path,
                                                 feature_grid,
                                                 year_model,
                                                 year_data))
        return feature_tasks


class UrbanCluster(CityGeneralTask):
    grid_tables_path = luigi.Parameter()
    year_model = luigi.Parameter()
    built_threshold = luigi.Parameter()
    population_threshold = luigi.Parameter()

    def requires(self):
        return GenerateFeaturesGrids(self.grid_tables_path)

    @property
    def query(self):
        return grids.urban_clusters(self.grid_size,
                                    self.city,
                                    self.built_threshold,
                                    self.population_threshold,
                                    self.year_model)

class UrbanClusters(luigi.Wrapper):
    grid_tables_path = configuration.get('general','grid_tables_path')
    urban_built_threshold = configuration.get('general','urban_built_threshold')
    urban_population_threshold = configuration.get('general', 'urban_population_threshold')
    dense_built_threshold = configuration.get('general', 'dense_built_threshold')
    dense_population_threshold = configuration.get('general', 'dense_population_threshold')

    def requires(self):
        years_model = utils.get_years_models()
        tasks = []
        # for urban clusters
        for year_model in years_model:
            tasks.append(UrbanCluster(self.grid_tables_path,
                                      year_model,
                                      self.urban_built_threshold,
                                      self.urban_population_threshold))
        # for dense clusters
        for year_model in years_model:
            tasks.append(UrbanCluster(self.grid_tables_path,
                                      year_model,
                                      self.dense_built_threshold,
                                      self.dense_population_threshold))
        return tasks


class UrbanFeatureGrid(CityGeneralTask):
    feature = luigi.Parameter()
    year_model = luigi.Parameter()
    built_threshold = luigi.Parameter()
    population_threshold = luigi.Parameter()
    cluster_threshold = luigi.Parameter()

    def requires(self):
        return UrbanClusters()

    @property
    def query(self):
        feature_function = eval(self.feature)
        return feature_function(self.grid_size,
                                self.city,
                                self.built_threshold,
                                self.population_threshold,
                                self.cluster_threshold,
                                self.year_mode)


class GenerateUrbanFeaturesGrids(luigi.Wrapper):
    grid_tables_path = configuration.get('general','grid_tables_path')
    urban_built_threshold = configuration.get('general','urban_built_threshold')
    urban_population_threshold = configuration.get('general', 'urban_population_threshold')
    urban_cluster_threshold = configuration.get('general', 'urban_cluster_threshold')
    dense_built_threshold = configuration.get('general', 'dense_built_threshold')
    dense_population_threshold = configuration.get('general', 'dense_population_threshold')
    dense_cluster_threshold = configuration.get('general', 'dense_cluster_threshold')

    def requires(self):
        ## TODO
        urbanfeatures = utils.urban_grids(self.grid_tables_path)
        tasks = []
        for feature_grid in urbanfeatures:
            ## TODO
            years_model = utils.get_years_models()
            for year_model in years_model:
                tasks.append(UrbanFeatureGrid(feature_grid,
                                              year_model,
                                              self.urban_built_threshold,
                                              self.urban_population_threshold,
                                              self.urban_cluster_threshold))

            for year_model in years_model:
                tasks.append(UrbanFeatureGrid(feature_grid,
                                              year_model,
                                              self.dense_built_threshold,
                                              self.dense_population_threshold,
                                              self.dense_cluster_threshold))
        return tasks
