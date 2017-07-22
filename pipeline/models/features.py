import pandas as pd
import json
import pdb
import sys

import os,sys,inspect
currentdir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parentdir = os.path.dirname(currentdir)
sys.path.insert(0,parentdir) 
import utils

def generate_features(city,
                      features,
                      features_table_name,
                      grid_size,
                      urban_built_threshold,
                      urban_population_threshold,
                      urban_cluster_threshold,
                      dense_built_threshold,
                      dense_population_threshold,
                      dense_cluster_threshold):

    # select statement of features
    selects = ["COALESCE({x},0) AS {x}".format(x=f)
                                      for f in features if 'urban_flag' not in f
                                      and 'urban_distance_km' not in f
                                      and 'high_density_distance' not in f]
    # urbans
    if 'high_density_distance_km' in features:
        selects.append("COALESCE(u2.urban_distance_km, 0) AS high_density_distance_km")
    if 'urban_distance_km' in features:
        selects.append("COALESCE(u1.urban_distance_km, 0) AS urban_distance_km")
    if 'urban_flag' in features:
        selects.append("COALESCE(u1.urban_flag, 0) AS urban_flag")

    #select_statement = ", ".join(selects)
    #DROP = ("""DROP TABLE IF EXISTS features.{city}_{prefix}_{size}"""
    #        .format(city=city,
    #                size=grid_size,
    #                prefix=features_table_name))
    QUERY = ("""CREATE table features.{city}_{prefix}_{size} AS (
                        SELECT cell_id,
                               year_model,
                               {selects}
                       FROM grids.{city}_highways_{size}
                       LEFT OUTER JOIN grids.{city}_slope_{size}
                            USING (cell_id)
                       LEFT OUTER JOIN grids.{city}_dem_{size}
                            USING (cell_id)
                       LEFT OUTER JOIN grids.{city}_geopins_{size}
                            USING (cell_id)
                       LEFT OUTER JOIN grids.{city}_water_bodies_{size}
                            USING (cell_id)
                       LEFT OUTER JOIN grids.{city}_urban_center_{size}
                            USING (cell_id)
                       LEFT OUTER JOIN grids.{city}_population_{size}
                            USING (cell_id)
                       LEFT OUTER JOIN grids.{city}_built_lds_{size}
                            USING (cell_id, year_model)
                       LEFT OUTER JOIN grids.{city}_city_lights_{size}
                            USING (cell_id, year_model)
                       LEFT OUTER JOIN grids.{city}_settlements_{size}
                            USING (cell_id, year_model)
                       LEFT OUTER JOIN grids.{city}_urban_distance_{size} u1
                            USING (cell_id, year_model)
                       LEFT OUTER JOIN grids.{city}_urban_distance_{size} u2
                            USING (cell_id, year_model)
                       LEFT OUTER JOIN grids.{city}_urban_neighbours_{size}
                            USING (cell_id, year_model)
                       WHERE u1.built_threshold = {u1_built_threshold}
                       AND  u1.population_threshold = {u1_population_threshold}
                       AND u1.cluster_threshold = {u1_cluster_threshold}
                       AND u2.built_threshold = {u2_built_threshold}
                       AND u2.population_threshold = {u2_population_threshold}
                       AND u2.cluster_threshold = {u2_cluster_threshold}
                  )""".format(city=city,
                             prefix=features_table_name,
                             size=grid_size,
                             selects=select_statement,
                             u1_built_threshold=urban_built_threshold,
                             u1_population_threshold=urban_population_threshold,
                             u1_cluster_threshold=urban_cluster_threshold,
                             u2_built_threshold=dense_built_threshold,
                             u2_population_threshold=dense_population_threshold,
                             u2_cluster_threshold=dense_cluster_threshold))

    #INDEX = ("""CREATE INDEX ON features.{city}_{prefix}_{size} (year_model)"""
    #           .format(city=city,
    #                  prefix=features_table_name,
    #                  size=grid_size))
    #db_conn = engine.raw_connection()
    #cur = db_conn.cursor()
    #cur.execute(DROP)
    #db_conn.commit()
    #cur.execute(QUERY)
    #db_conn.commit()
    #cur.execute(INDEX)
    #db_conn.commit()
    #db_conn.close()
    return QUERY


def generate_labels(city,
                    labels_table_name,
                    years,
                    grid_size,
                    built_threshold,
                    population_threshold,
                    cluster_threshold):

    #DROP = ("""DROP TABLE IF EXISTS features.{city}_{prefix}_{size}"""
    #            .format(city=city,
    #                    prefix=labels_table_name,
    #                    size=grid_size))
    subqueries_list = []
    selects_list = []
    for i in range(len(years)-1):
        subqueries_list.append("""labels_{year} AS (
                                  SELECT cell_id,
                                         {year} as year_model,
                                         max(st_within(cell, geom)::int) as label
                            FROM grids.{city}_grid_{size}, grids.{city}_urban_clusters_{size}
                            WHERE built_threshold = {built_threshold}
                            AND population_threshold = {population_threshold}
                            AND population >= {cluster_threshold}
                            AND year_model = {next_year}
                            GROUP BY cell_id, year_model )
                            """.format(year=years[i],
                                      city=city,
                                      size=grid_size,
                                      next_year=years[i+1],
                                      built_threshold=built_threshold,
                                      population_threshold=population_threshold,
                                      cluster_threshold=cluster_threshold))

        selects_list.append("""SELECT * FROM labels_{year}""".format(year=years[i]))

    subqueries = ", ".join(subqueries_list)
    selects = " UNION ".join(selects_list)
    QUERY_LABELS = ("""CREATE TABLE features.{city}_{prefix}_{size} AS (
                        WITH {subqueries}
                              {selects})""".format(city=city,
                                                   prefix=labels_table_name,
                                                   size=grid_size,
                                                   subqueries=subqueries,
                                                   selects=selects))
    # Create index on years for labels
    # INDEX = ("""CREATE INDEX ON features.{city}_{prefix}_{size} (year_model)"""
    #           .format(city=city,
    #                   prefix=labels_table_name,
    #                   size=grid_size))
    # # get connection and send queries
    # db_conn = engine.raw_connection()
    # cur = db_conn.cursor()
    # cur.execute(DROP)
    # db_conn.commit()
    # cur.execute(QUERY_LABELS)
    # db_conn.commit()
    # cur.execute(INDEX)
    # db_conn.commit()
    return QUERY_LABELS

if __name__ == "__main__":
    # PArams
    city = 'amman'
    grid_size = 250
    urban_built_threshold = 50
    urban_population_threshold = 75
    urban_cluster_threshold = 5000
    dense_built_threshold = 50
    dense_population_threshold = 375
    dense_cluster_threshold = 5000
    features_table_name = 'features'
    years = [1990, 2000, 2014]
    labels_table_name = 'labels'

    # read experiment
    experiment_path = '../experiment.yaml'
    experiment = utils.read_yaml(experiment_path)
    # read features
    features = utils.get_features(experiment)
    # engine
    engine = utils.get_engine()
    # run features
    generate_features(engine,
                      city,
                      features,
                      features_table_name,
                      grid_size,
                      urban_built_threshold,
                      urban_population_threshold,
                      urban_cluster_threshold,
                      dense_built_threshold,
                      dense_population_threshold,
                      dense_cluster_threshold)
    generate_labels(engine,
                    city,
                    labels_table_name,
                    years,
                    grid_size,
                    urban_built_threshold,
                    urban_population_threshold,
                    urban_cluster_threshold)

