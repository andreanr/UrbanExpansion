import pandas as pd
import json
import pdb
import hashlib
import numpy as np
from sklearn import (svm, ensemble, tree,
                     linear_model, neighbors, naive_bayes)

import utils

def get_data(db_engine,
             years,
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
                 WHERE year_model IN ({years})"""
                .format(features=", ".join(features),
                        features_table_name=features_table_name,
                        labels_table_name=labels_table_name,
                        size=grid_size,
                        city=city,
                        years=",".join([str(x) for x in years])))

    data = pd.read_sql(query, db_engine)
    data.set_index('cell_id', inplace=True)
    return np.array(data.ix[:, data.columns != 'label']), np.array(data['label']), data.index


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


def predict_model(modelobj, test):
    predicted_score = modelobj.predict_proba(test)[:, 1]
    return predicted_score


def store_train(timestamp,
                model,
                city,
                parameters,
                features,
                years_train,
                grid_size,
                built_threshold,
                population_threshold,
                cluster_threshold,
                label_range,
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
                                             years_train,
                                             grid_size,
                                             built_threshold,
                                             population_threshold,
                                             cluster_threshold,
                                             label_range,
                                             model_comment)
                VALUES ('{run_time}'::TIMESTAMP,
                        '{city}',
                        '{model_type}',
                        '{model_parameters}',
                         ARRAY{features},
                         ARRAY{years_train},
                         '{grid_size}',
                         {built_threshold},
                         {population_threshold},
                         {cluster_threshold},
                         ARRAY{label_range},
                         '{model_comment}') """.format(run_time=timestamp,
                                                       city=city,
                                                      model_type=model,
                                                      model_parameters=json.dumps(dict(parameters)),
                                                      features=list(features),
                                                      years_train=years_train,
                                                      grid_size=grid_size,
                                                      built_threshold=built_threshold,
                                                      population_threshold=population_threshold,
                                                      cluster_threshold=cluster_threshold,
                                                      label_range=label_range,
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
                         AND model_type = '{model}'
                         AND city = '{city}'
                         AND model_parameters = '{parameters}'"""
                .format(timestamp=timestamp,
                       model=model,
                       city=city,
                       parameters=json.dumps(parameters)))
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
                      year_features,
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
                                          "year_features": year_features,
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


def store_evaluations(engine, model_id, city, years, type_validation, metrics):
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
                                                    years,
                                                    type_validation,
                                                    metric,
                                                    cutoff,
                                                    value)
                   VALUES( {0}, '{1}', ARRAY{2}, '{3}',  '{4}', {5}, {6}) """
                   .format( model_id,
                            city,
                            years,
                            type_validation,
                            metric,
                            metric_cutoff,
                            evaluation ))

        db_conn.cursor().execute(query)
        db_conn.commit()


def define_model(model, parameters, n_cores=1):
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

def generate_uuid(parameters):
    """ Generate a unique identifier given a dictionary or list
    :param metadata: metadata for the matrix
    :returns: unique name for the file
    """
    def dt_handler(x):
        if isinstance(x, datetime.datetime) or isinstance(x, datetime.date):
            return x.isoformat()
        raise TypeError("Unknown type")

    if isinstance(parameters, list):
        return hashlib.md5(
                    json.dumps(sorted(parameters), default=dt_handler, sort_keys=True)
                     .encode('utf-8')
                 ).hexdigest()

    elif isinstance(parameters, dict):
         return hashlib.md5(
                    json.dumps(parameters, default=dt_handler, sort_keys=True)
                    .encode('utf-8')
                ).hexdigest()

