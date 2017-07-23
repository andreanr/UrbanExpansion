import pandas as pd
import json
import pdb
import numpy as np
from sklearn import (svm, ensemble, tree,
                     linear_model, neighbors, naive_bayes)

import utils

def get_data(db_engine,
             year,
             city,
             features,
             grid_size,
             features_table_prefix,
             labels_table_prefix):
    """
    Function that given the citu, the grid size, the year,
    the features table prefix, labels table prefix and
    the selected features specified on the experiment yaml it
    returns a dataframe of features and labels to run models

    """
    features_table_name = '{city}_{prefix}_{size}'.format(city=city,
                                                          prefix=features_table_prefix,
                                                          size=grid_size)

    labels_table_name = '{city}_{prefix}_{size}'.format(city=city,
                                                        prefix=labels_table_prefix,
                                                        size=grid_size)

    query = ("""SELECT cell_id, {features}, label::bool
                FROM  features.{features_table_name}
                LEFT OUTER JOIN features.{labels_table_name}
                USING (cell_id, year_model)
                 WHERE year_model = '{year}'"""
                .format(features=", ".join(features),
                        features_table_name=features_table_name,
                        labels_table_name=labels_table_name,
                        size=grid_size,
                        city=city,
                        year=year))

    data = pd.read_sql(query, db_engine)
    data.set_index('cell_id', inplace=True)
    return data.ix[:, data.columns != 'label'], data['label']

def store_train(timestamp,
                model,
                city,
                parameters,
                features,
                year_train,
                grid_size,
                built_threshold,
                population_threshold,
                cluster_threshold,
                model_comment):
    """
    Function that stores all train information
    for each model on results.models on the database
    """

    query = (""" INSERT INTO results.models (run_time,
                                             city,
                                             model_type,
                                             model_parameters,
                                             features,
                                             year_train,
                                             grid_size,
                                             built_threshold,
                                             population_threshold,
                                             cluster_threshold,
                                             model_comment)
                VALUES ('{run_time}'::TIMESTAMP,
                        '{city}',
                        '{model_type}',
                        '{model_parameters}',
                         ARRAY{features},
                         '{year_train}',
                         '{grid_size}',
                         {built_threshold},
                         {population_threshold},
                         {cluster_threshold},
                         '{model_comment}') """.format(run_time=timestamp,
                                                       city=city,
                                                      model_type=model,
                                                      model_parameters=json.dumps(parameters),
                                                      features=features,
                                                      year_train=year_train,
                                                      grid_size=grid_size,
                                                      built_threshold=built_threshold,
                                                      population_threshold=population_threshold,
                                                      cluster_threshold=cluster_threshold,
                                                      model_comment=model_comment))
    return query


def get_model_id(db_engine, model, city, parameters, timestamp):
    """
    Function that given the model, city, parameters and timestamp
    returns the model id (int) of the train model stored on
    results.models
    """
    db_conn = db_engine.raw_connection()
    query_model_id = ("""SELECT model_id as id from results.models
                         WHERE run_time ='{timestamp}'::timestamp
                         AND model = '{model}'
                         AND city = '{city}'
                         AND parameters = '{parameters}'"""
                .format(timestamp=timestamp,
                       model=model,
                       city=city,
                       parameters=parameters))
    model_id = pd.read_sql(query_model_id, db_engine)
    db_conn.close()
    return model_id['id'].iloc[0]

def store_importances(db_engine, model_id, city, features, importances):
    """
    Functions that stores all the feature importantes for each model
    on the db on results.feature_importances
    """
    # Create pandas db of features importance
    dataframe_for_insert = pd.DataFrame( {"model_id": model_id,
                                          "city": city,
                                          "feature": features,
                                          "feature_importance": importances})

    # generate ranks
    dataframe_for_insert['rank_abs'] = dataframe_for_insert['feature_importance'].rank(method='dense',
                                                                                       ascending=False)
    dataframe_for_insert.to_sql("feature_importances",
                                 db_engine,
                                 if_exists="append",
                                 schema="results",
                                 index=False )
    return True


def store_predictions(db_engine,
                      model_id,
                      city,
                      year_test,
                      cell_id,
                      scores,
                      test_y):
    """
    Stores predictions made for each model
    on the database on results.predictions
    """
    # Create pandas db of features importance
    dataframe_for_insert = pd.DataFrame( {"model_id": model_id,
                                          "city": city,
                                          "year_test": year_test,
                                          "cell_id": cell_id,
                                          "score": scores,
                                          "label": test_y})
    # round score value
    dataframe_for_insert['score'] = dataframe_for_insert['score'].apply(lambda x: round(x,5))
    dataframe_for_insert.to_sql("predictions",
                                db_engine,
                                if_exists="append",
                                schema="results",
                                index=False,
                                chunksize=500 )
    return True


def store_evaluations(engine, model_id, city, year_test, metrics):
    """
    Functions that stores the evauation metric for the year test
    on results.evaluations
    """
    db_conn = engine.raw_connection()
    for key in metrics:
        evaluation = metrics[key]
        metric = key.split('|')[0]
        try:
            metric_cutoff = key.split('|')[1]
            if metric_cutoff == '':
                metric_cutoff.replace('', None)
            else:
                pass
        except:
            metric_cutoff = None

        # store
        if metric_cutoff is None:
            metric_cutoff = 'Null'
        query = ("""INSERT INTO results.evaluations(model_id,
                                                    city,
                                                    year_test,
                                                    metric,
                                                    cutoff,
                                                    value)
                   VALUES( {0}, '{1}', {2}, '{3}', {4}, {5}) """
                   .format( model_id,
                            city,
                            year_test,
                            metric,
                            metric_cutoff,
                            evaluation ))

        db_conn.cursor().execute(query)
        db_conn.commit()


def define_model(model, parameters, n_cores):
    """
    Function that given the model name,
    calls the model object with the
    specifief parameters
    """
    if model == "RandomForest":
        return ensemble.RandomForestClassifier(
            n_estimators=parameters['n_estimators'],
            max_features=parameters['max_features'],
            criterion=parameters['criterion'],
            max_depth=parameters['max_depth'],
            min_samples_split=parameters['min_samples_split'],
            random_state=parameters['random_state'],
            n_jobs=n_cores)

    elif model == 'SVM':
        return svm.SVC(C=parameters['C_reg'],
                       kernel=parameters['kernel'],
                       probability=True)

    elif model == 'LogisticRegression':
        return linear_model.LogisticRegression(
            C=parameters['C_reg'],
            random_state=parameters['random_state'],
            penalty=parameters['penalty'])

    elif model == 'AdaBoost':
        return ensemble.AdaBoostClassifier(
            learning_rate=parameters['learning_rate'],
            algorithm=parameters['algorithm'],
            n_estimators=parameters['n_estimators'])

    elif model == 'ExtraTrees':
        return ensemble.ExtraTreesClassifier(
            n_estimators=parameters['n_estimators'],
            max_features=parameters['max_features'],
            criterion=parameters['criterion'],
            max_depth=parameters['max_depth'],
            min_samples_split=parameters['min_samples_split'],
            random_state=parameters['random_state'],
            n_jobs=n_cores)

    elif model == 'GradientBoostingClassifier':
        return ensemble.GradientBoostingClassifier(
            n_estimators=parameters['n_estimators'],
            learning_rate=parameters['learning_rate'],
            subsample=parameters['subsample'],
            max_depth=parameters['max_depth'])

    elif model == 'GaussianNB':
        return naive_bayes.GaussianNB()

    elif model == 'DecisionTreeClassifier':
        return tree.DecisionTreeClassifier(
            max_features=parameters['max_features'],
            criterion=parameters['criterion'],
            max_depth=parameters['max_depth'],
            min_samples_split=parameters['min_samples_split'])

    elif model == 'SGDClassifier':
        return linear_model.SGDClassifier(
            loss=parameters['loss'],
            penalty=parameters['penalty'],
            n_jobs=n_cores)

    elif model == 'KNeighborsClassifier':
        return neighbors.KNeighborsClassifier(
            n_neighbors=parameters['n_neighbors'],
            weights=parameters['weights'],
            algorithm=parameters['algorithm'],
            n_jobs=n_cores)

    else:
        raise ConfigError("Unsupported model {}".format(model))
