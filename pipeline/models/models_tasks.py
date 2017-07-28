import luigi
import datetime
import pdb
from itertools import product
from luigi.contrib import postgres
from luigi import configuration

from models.features_tasks import FeatureGenerator, LabelGenerator
from models import model_utils
from models import scoring
from commons import city_task
from dotenv import find_dotenv, load_dotenv

import utils

load_dotenv(find_dotenv())


class TrainModel(city_task.FeaturesTask):
    """
    This class takes in the model name and
    parameters as well as all city task parameters
    to train, test, predict and store

    Args:
      model (str): model name
      parameters (dict): dictionary for parameters to fit model
    """
    model = luigi.Parameter()
    parameters = luigi.DictParameter()
    features = luigi.ListParameter()
    timestamp = datetime.datetime.now()
    table = 'results.models'

    def requires(self):
        yield [FeatureGenerator(self.features),
               LabelGenerator()]

    @property
    def query(self):
        """
        Returns the query for storing the
        train information on results schema
        """
        return model_utils.store_train(
                             self.timestamp,
                             self.model,
                             self.city,
                             self.parameters,
                             self.features,
                             self.year_train,
                             self.grid_size,
                             self.urban_built_threshold,
                             self.urban_population_threshold,
                             self.urban_cluster_threshold,
                             self.model_comment)

    def run(self):
        engine = utils.get_engine()
        connection = engine.raw_connection()
        cursor = connection.cursor()

        # commit and close connection
        print('Get data')
        train_x, train_y = model_utils.get_data(engine,
                                       self.year_train,
                                       self.city,
                                       self.features,
                                       self.grid_size,
                                       self.features_table_prefix,
                                       self.labels_table_prefix)

        parameters = dict(self.parameters)
        modelobj = model_utils.define_model(self.model, parameters)
        print('fit model')
        modelobj.fit(train_x, train_y)

        print('get feature importances')
        importances = model_utils.get_feature_importances(modelobj)
        sql = self.query
        cursor.execute(sql)
        connection.commit()

        # get model_id
        model_id = model_utils.get_model_id(engine, self.model, self.city, parameters, self.timestamp)
        model_utils.store_importances(engine, model_id, self.city, self.features, importances)

        # Testing
        if self.year_test:
            print('testing')
            test_x, test_y  = model_utils.get_data(engine,
                                          self.year_test,
                                          self.city,
                                          self.features,
                                          self.grid_size,
                                          self.features_table_prefix,
                                          self.labels_table_prefix)
            test_x['scores'] = model_utils.predict_model(modelobj, test_x)
            model_utils.store_predictions(engine,
                                          model_id,
                                          self.city,
                                          self.year_test,
                                          test_x.index,
                                          test_x['scores'],
                                          test_y)
            print('scoring')
            metrics = scoring.calculate_all_evaluation_metrics(test_y,
                                                               test_x['scores'])
            model_utils.store_evaluations(engine,
                                          model_id,
                                          self.city,
                                          self.year_test,
                                          metrics)
        # Predicting
        if self.year_predict:
            print('predicting')
            predict_x, predict_y = model_utils.get_data(engine,
                                                         self.year_predict,
                                                         self.city,
                                                         self.features,
                                                         self.grid_size,
                                                         self.features_table_prefix,
                                                         self.labels_table_prefix)
            predict_x['scores'] = model_utils.predict_model(modelobj, predict_x)
            model_utils.store_predictions(engine,
                                          model_id,
                                          self.city,
                                          self.year_predict,
                                          predict_x.index,
                                          predict_x['scores'],
                                          predict_y)

        # Update marker table
        self.output().touch(connection)
        connection.commit()
        connection.close()


class TrainModels(luigi.WrapperTask):
    """
    Luigi Wrapper that loops across all models
    and all combination of parameters specified
    on the experiment yaml file

    Args:
        models (list): list of models to run
        parameters (dict): combination of grid parameters
                           for running the models
    """
    features = luigi.ListParameter()
    models = luigi.ListParameter()
    parameters = luigi.DictParameter()

    def requires(self):
        tasks = []
        # loop through models list
        for model in self.models:
            parameter_names = sorted(self.parameters[model])
            parameter_values = [list(self.parameters[model][p]) for p in parameter_names]
            all_params = product(*parameter_values)
            # loop through combination of parameters for each model
            for each_param in all_params:
                param_i = {name: value for name, value in zip(parameter_names, each_param)}
                tasks.append(TrainModel(model, param_i, self.features))
        yield tasks

