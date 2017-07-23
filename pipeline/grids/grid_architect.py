import logging
import luigi
from luigi import configuration
import pdb

from grids.generate_grids import GenerateGridTables, AddPrimaryKey
from grids.grids import *
from commons import city_task
import utils

class GridFeature(city_task.CityGeneralTask):
    """
    Task that Inserts values to a feature grid
    given year_model, year_data and feature
    """
    grid_tables_path = luigi.Parameter()
    feature = luigi.Parameter()
    year_model = luigi.Parameter()
    year_data = luigi.Parameter()

    def requires(self):
       return [GenerateGridTables(self.city, self.grid_size, self.grid_tables_path),
               AddPrimaryKey(self.city, self.esri, self.grid_size)]

    @property
    def table(self):
        return """grids.{city}_{feature}_{size}""".format(city=self.city,
                                                          feature=self.feature,
                                                          size=self.grid_size)
    @property
    def query(self):
        feature_function = eval(self.feature)
        if self.year_model:
            return feature_function(self.grid_size,
                                    self.city,
                                    self.esri,
                                    self.year_data,
                                    self.year_model)
        else:
            return feature_function(self.grid_size,
                                    self.city,
                                    self.esri)


class GenerateGridsFeatures(luigi.WrapperTask):
    """
    Task that loops GridFeature for all
    non urban features
    """
    grid_tables_path = luigi.Parameter()

    def requires(self):
        # Lit of nonurban features
        nonurban_features = utils.nonurban_grids(self.grid_tables_path)
        feature_tasks = []
        for feature_grid in nonurban_features:
            ## returns a dict of {year_model: year_data}
            years_dict = utils.get_features_years(feature_grid)
            if len(years_dict) > 0:
                for year_model, year_data in years_dict.items():
                    feature_tasks.append(GridFeature(self.grid_tables_path,
                                                     feature_grid,
                                                     year_model,
                                                     year_data))
            else:
                feature_tasks.append(GridFeature(self.grid_tables_path,
                                                feature_grid,
                                                "",""))

        return feature_tasks


class UrbanCluster(city_task.CityGeneralTask):
    grid_tables_path = luigi.Parameter()
    year_model = luigi.Parameter()
    built_threshold = luigi.Parameter()
    population_threshold = luigi.Parameter()

    def requires(self):
        return GenerateGridsFeatures(self.grid_tables_path)

    @property
    def table(self):
        return """grids.{city}_urban_clusters_{size}""".format(city=self.city,
                                                               size=self.grid_size)
    @property
    def query(self):
        return grids.urban_clusters(self.grid_size,
                                    self.city,
                                    self.built_threshold,
                                    self.population_threshold,
                                    self.year_model)

class UrbanClusters(luigi.WrapperTask):
    grid_tables_path = configuration.get_config().get('general','grid_tables_path')
    urban_built_threshold = configuration.get_config().get('general','urban_built_threshold')
    urban_population_threshold = configuration.get_config().get('general', 'urban_population_threshold')
    dense_built_threshold = configuration.get_config().get('general', 'dense_built_threshold')
    dense_population_threshold = configuration.get_config().get('general', 'dense_population_threshold')

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


class UrbanGridFeature(city_task.CityGeneralTask):
    feature = luigi.Parameter()
    year_model = luigi.Parameter()
    built_threshold = luigi.Parameter()
    population_threshold = luigi.Parameter()
    cluster_threshold = luigi.Parameter()

    def requires(self):
        return UrbanClusters()

    @property
    def table(self):
        return """grids.{city}_{feature}_{size}""".format(city=self.city,
                                                          feature=self.feature,
                                                          size=self.grid_size)
    @property
    def query(self):
        feature_function = eval(self.feature)
        return feature_function(self.grid_size,
                                self.city,
                                self.built_threshold,
                                self.population_threshold,
                                self.cluster_threshold,
                                self.year_mode)


class GenerateUrbanGridsFeatures(luigi.WrapperTask):
    grid_tables_path = configuration.get_config().get('general','grid_tables_path')
    urban_built_threshold = configuration.get_config().get('general','urban_built_threshold')
    urban_population_threshold = configuration.get_config().get('general', 'urban_population_threshold')
    urban_cluster_threshold = configuration.get_config().get('general', 'urban_cluster_threshold')
    dense_built_threshold = configuration.get_config().get('general', 'dense_built_threshold')
    dense_population_threshold = configuration.get_config().get('general', 'dense_population_threshold')
    dense_cluster_threshold = configuration.get_config().get('general', 'dense_cluster_threshold')

    def requires(self):
        ## TODO
        urbanfeatures = utils.urban_grids(self.grid_tables_path)
        tasks = []
        for feature_grid in urbanfeatures:
            ## TODO
            years_model = utils.get_years_models()
            for year_model in years_model:
                tasks.append(UrbanGridFeature(feature_grid,
                                              year_model,
                                              self.urban_built_threshold,
                                              self.urban_population_threshold,
                                              self.urban_cluster_threshold))

            for year_model in years_model:
                tasks.append(UrbanGridFeature(feature_grid,
                                              year_model,
                                              self.dense_built_threshold,
                                              self.dense_population_threshold,
                                              self.dense_cluster_threshold))
        return tasks
