import luigi
import datetime
import pdb

from itertools import product
from luigi.contrib import postgres
from luigi import configuration
from sklearn.model_selection import KFold
from dotenv import find_dotenv, load_dotenv

from models.features_tasks import FeatureGenerator, LabelGenerator
from models import model_utils
from models import scoring
from commons import city_task

import utils

load_dotenv(find_dotenv())


class CVModel(city_task.FeaturesTask):
    model = luigi.Parameter()
    parameters = luigi.DictParameter()
    features = luigi.ListParameter()
    timestamp = datetime.datetime.now()
    table = 'results.models'
    n_folds = int(configuration.get_config().get('general','n_folds'))

    @property
    def update_id(self):
        hash_parameters = model_utils.generate_uuid(dict(self.parameters))
        hash_features = model_utils.generate_uuid(self.features)
        return ("""CVModel__{city}:{size}_{model}:{params}:{feat}:{years}:{folds}:{built}:{pop}:{cluster}"""
                    .format(city=self.city,
                            size=self.grid_size,
                            model=self.model,
                            params=hash_parameters,
                            feat=hash_features,
                            years="-".join([str(x) for x in self.years_train]),
                            folds=self.n_folds,
                            built=self.urban_built_threshold,
                            pop=self.urban_population_threshold,
                            cluster=self.urban_cluster_threshold))
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
                             self.years_train,
                             self.grid_size,
                             self.urban_built_threshold,
                             self.urban_population_threshold,
                             self.urban_cluster_threshold,
                             self.label_range,
                             self.model_comment)

    def requires(self):

        yield [FeatureGenerator(self.features),
               LabelGenerator()]

    def run(self):
        engine = utils.get_engine()
        connection = engine.raw_connection()
        cursor = connection.cursor()

        sql = self.query
        cursor.execute(sql)
        connection.commit()
        self.output().touch(connection)
        connection.commit()

        print('Get data')
        X, y, X_indexes = model_utils.get_data(engine,
                                    self.years_train,
                                    self.city,
                                    self.features,
                                    self.grid_size,
                                    self.features_table_prefix,
                                    self.labels_table_prefix)

        parameters = dict(self.parameters)
        # Train with all data
        modelobj = model_utils.define_model(self.model, parameters)
        print('fit model')
        modelobj.fit(X, y)

        print('get feature importances')
        importances = model_utils.get_feature_importances(modelobj)

        # kfolds 
        kf = KFold(n_splits=self.n_folds)
        kf.get_n_splits(X)
        folds_metrics = dict()
        folds = 1
        for train_index, test_index in kf.split(X):
            X_train, X_test = X[train_index], X[test_index]
            y_train, y_test = y[train_index], y[test_index]
            # Train
            print('train model for fold {0}'.format(folds))
            modelobj_f = model_utils.define_model(self.model, parameters)
            modelobj_f.fit(X_train, y_train)
            # Test
            print('test model for fold {0}'.format(folds))
            scores = model_utils.predict_model(modelobj_f, X_test)
            # Scoring
            folds_metrics[folds] = scoring.calculate_all_evaluation_metrics(y_test, scores)
            folds += 1

        # get model_id
        model_id = model_utils.get_model_id(engine, self.model, self.city, parameters, self.timestamp)
        model_utils.store_importances(engine, model_id, self.city, self.features, importances)

        # Obtain averages of metrics for all folds
        metrics = scoring.cv_evaluation_metrics(folds_metrics)
        model_utils.store_evaluations(engine,
                                      model_id,
                                      self.city,
                                      self.years_train,
                                      'cv',
                                      metrics)

        if self.year_predict:
            print('predicting')
            predict_x, predict_y, predict_index = model_utils.get_data(engine,
                                                        [self.year_predict],
                                                        self.city,
                                                        self.features,
                                                        self.grid_size,
                                                        self.features_table_prefix,
                                                        self.labels_table_prefix)
            scores = model_utils.predict_model(modelobj, predict_x)
            model_utils.store_predictions(engine,
                                          model_id,
                                          self.city,
                                          self.year_predict,
                                          predict_index,
                                          scores,
                                          predict_y)
        connection.close()

class RunCVModels(luigi.WrapperTask):
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
                tasks.append(CVModel(model, param_i, self.features))
        return tasks
