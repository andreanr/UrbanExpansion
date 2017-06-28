import pandas as pd
import json
import pdb

def get_features(experiment):
    return [x for x in experiment['Features'] if experiment['Features'][x]==True]


def get_data(year,
             city,
             features,
             grid_size,
             features_table_prefix,
             labels_table_prefix,
             intersect_percent):

    features_table_name = '{city}_{prefix}_{size}'.format(city=city,
                                                          prefix=features_table_prefix,
                                                          grid_size=grid_size)

    labels_table_name = '{city}_{prefix}_{percent}_{size}'.format(city=city,
                                                                  prefix=labels_table_prefix,
                                                                  percent=intersect_percent,
                                                                  size=grid_size)

    query = ("""SELECT cell_id, {features}, label::bool
                FROM  features.{features_table_name}
                JOIN features.{labels_table_name}
                USING (cell_id, year)
                JOIN grid_{size}.{city}_grid
                USING (cell_id)
                JOIN preprocess.{city}_buffer_{year}
                ON st_intersects(cell, buffer_geom)
                 WHERE year = '{year}'"""
                .format(features=", ".join(features),
                        features_table_name=features_table_name,
                        labels_table_name=labels_table_name,
                        size=grid_size,
                        city=city,
                        year=year))

    db_engine = get_connection()
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
                intersect_percent,
                model_comment):

    query = (""" INSERT INTO results.models (run_time,
                                             city,
                                             model_type,
                                             model_parameters,
                                             features,
                                             year_train,
                                             grid_size,
                                             intersect_percent,
                                             model_comment)
                VALUES ('{run_time}'::TIMESTAMP,
                        '{city}',
                        '{model_type}',
                        '{model_parameters}',
                         ARRAY{features},
                         '{year_train}',
                         '{grid_size}',
                         {intersect_percent},
                         '{model_comment}') """.format(run_time=timestamp,
                                                       city=city,
                                                      model_type=model,
                                                      model_parameters=json.dumps(parameters),
                                                      features=features,
                                                      year_train=year_train,
                                                      grid_size=grid_size,
                                                      intersect_percent=intersect_percent,
                                                      model_comment=model_comment))
    db_conn = get_connection().raw_connection()
    cur = db_conn.cursor()
    cur.execute(query)
    db_conn.commit()

    # return model_id
    query_model_id = """SELECT model_id as id from results.models where run_time = timestamp """
    db_engine = get_connection()
    model_id = pd.read_sql(query_model_id, db_engine)
    return model_id['id'].iloc[0]

def store_importances(model_id, city, features, importances):

    # Create pandas db of features importance
    dataframe_for_insert = pd.DataFrame( {  "model_id": model_id,
                                            "city": city,
                                            "feature": features,
                                            "feature_importance": importances})

    dataframe_for_insert['rank_abs'] = dataframe_for_insert['feature_importance'].rank(method='dense',
                                                                                       ascending=False)
    db_engine = get_connection()
    dataframe_for_insert.to_sql("feature_importances",
                                 db_engine,
                                 if_exists="append",
                                 schema="results",
                                 index=False )
    return True


def store_predictions(model_id,
                      city,
                      year_test,
                      cell_id,
                      scores,
                      test_y):

    dataframe_for_insert = pd.DataFrame( {"model_id": model_id,
                                          "city": city,
                                          "year_test": year_test,
                                          "cell_id": cell_id,
                                          "score": scores,
                                          "label": test_y})
    dataframe_for_insert['score'] = dataframe_for_insert['score'].apply(lambda x: round(x,5))

    db_engine = get_connection()
    dataframe_for_insert.to_sql("predictions",
                                db_engine,
                                if_exists="append",
                                schema="results",
                                index=False )
    return True
