import yaml
import pandas as pd
import datetime
import pdb
from itertools import product
import numpy as np
import psycopg2
from sklearn import (svm, ensemble, tree,
                     linear_model, neighbors, naive_bayes)

import model_utils as mu

import os,sys,inspect
currentdir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parentdir = os.path.dirname(currentdir)
sys.path.insert(0,parentdir)
import utils


def gen_models_to_run(engine, city,
                      grid_size,
                      features,
                      features_table_prefix,
                      labels_table_prefix,
                      models,
                      parameters,
                      model_comment,
                      built_threshold,
                      population_threshold,
                      cluster_threshold,
                      year_train=1990,
                      year_test=2000,
                      year_predict=2014):

    # get data
    train_x, train_y = mu.get_data(engine,
                                   year_train,
                                    city,
                                      features,
                                      grid_size,
                                      features_table_prefix,
                                      labels_table_prefix)

    # Magic Loop
    for model in models:
        print('Using model: {}'.format(model))
        parameter_names = sorted(parameters[model])
        parameter_values = [parameters[model][p] for p in parameter_names]
        all_params = product(*parameter_values)

        for each_param in all_params:
            print('With parameters: {}'.format(each_param))
            print('-----------------------------')
            timestamp = datetime.datetime.now()
            parameters = {name: value for name, value
                              in zip(parameter_names, each_param)}
            # Train
            print('training')
            modelobj, importances = train(train_x,
                                          train_y,
                                          model,
                                          parameters,
                                          2)
            # Store model
            model_id = mu.store_train(engine,
                                     timestamp,
                                          model,
                                          city,
                                          parameters,
                                          features,
                                          year_train,
                                          grid_size,
                                          built_threshold,
                                          population_threshold,
                                          cluster_threshold,
                                          model_comment)

            print('Model id: {}'.format(model_id))
            mu.store_importances(engine, model_id,city, features, importances)

            print('testing')
            if year_test:
                test_x, test_y  = mu.get_data(engine,
                                              year_test,
                                              city,
                                                 features,
                                                 grid_size,
                                                 features_table_prefix,
                                                 labels_table_prefix)
                test_x['scores'] = predict_model(modelobj, test_x)
                mu.store_predictions(engine,
                                     model_id,
                                     city,
                                         year_test,
                                         test_x.index,
                                         test_x['scores'],
                                         test_y)

            print('predicting')
            if year_predict:
                predict_x, predict_y  = mu.get_data(engine,
                                                    year_predict,
                                                    city,
                                                       features,
                                                       grid_size,
                                                       features_table_prefix,
                                                       labels_table_prefix)
                predict_x['scores'] = predict_model(modelobj, predict_x)
                mu.store_predictions(engine,
                                     model_id,
                                     city,
                                         year_predict,
                                         predict_x.index,
                                         predict_x['scores'],
                                         predict_y)

    print('Done!')


def get_feature_importances(model):
    """
    Get feature importances (from scikit-learn) of trained model.
    Args:
        model: Trained model
    Returns:
        Feature importances, or failing that, None
    """
    ##TODO return a dict
    try:
        return model.feature_importances_
    except:
        pass
    try:
        # Must be 1D for feature importance plot
        if len(model.coef_) <= 1:
            return model.coef_[0]
        else:
            return model.coef_
    except:
        pass
    return None


def train(train_x, train_y, model, parameters, n_cores):
    modelobj = mu.define_model(model, parameters, n_cores)
    modelobj.fit(train_x, train_y)

    importances = get_feature_importances(modelobj)
    return modelobj, importances


def predict_model(modelobj, test):
    predicted_score = modelobj.predict_proba(test)[:, 1]
    return predicted_score


if __name__ == '__main__':

    city = 'amman'
    grid_size = 250
    urban_built_threshold = 50
    urban_population_threshold = 75
    urban_cluster_threshold = 5000
    features_table_name = 'features'
    years = [1990, 2000, 2014]
    labels_table_name = 'labels'
    model_comment = 'test'

    # read experiment
    experiment_path = '../experiment.yaml'
    experiment = utils.read_yaml(experiment_path)
    features = utils.get_features(experiment)
    models = experiment['models']
    parameters = experiment['parameters']
    engine = utils.get_engine()

    gen_models_to_run(engine, city,
                      grid_size,
                      features,
                      features_table_name,
                      labels_table_name,
                      models,
                      parameters,
                      model_comment,
                      urban_built_threshold,
                      urban_population_threshold,
                      urban_cluster_threshold,
                      year_train=1990,
                      year_test=2000,
                      year_predict=2014)
