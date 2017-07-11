import pandas as pd
import json
import pdb

from . import utils


def generate_features(engine,
                      city,
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
        selects <- ["COALESCE({x},0) AS {x}".format(f)
                                      for f in features if 'urban' not in f
                                      and 'high_density_distance' not in f]
        # urbans
        if 'high_density_distance_km' in features:
            selects.append("COALESCE(u2.urban_distance_km, 0) AS high_density_distance")
        if 'urban_distance_km' in features:
             selects.append("COALESCE(u1.urban_distance_km, 0) AS urban_distance")
        if 'urban_flag' in features:
            selects.append("COALESCE(u1.urban_flag, 0) AS urban_flag")

        select_statement = ", ".join(selects)
        QUERY = ("""CREATE table features.{city}_{prefix}_{size}_{threshold} AS (
                        SELECT cell_id,
                               year_model,
                               {selects}
                       JOIN grids.{city}_highways_{size}
                            USING (cell_id)
                       JOIN grids.{city}_slope_{size}
                            USING (cell_id)
                       JOIN grids.{city}_dem_{size}
                            USING (cell_id)
                       JOIN grids.{city}_geopins_{size}
                            USING (cell_id)
                       JOIN grids.{city}_water_bodies_{size}
                            USING (cell_id)
                       JOIN grids.{city}_urban_center_{size}
                            USING (cell_id)
                       JOIN grids.{city}_population_{size}
                            USING (cell_id)
                       JOIN grids.{city}_built_lds_{size}
                            USING (cell_id, year_model)
                       JOIN grids.{city}_city_lights_{size}
                            USING (cell_id, year_model)
                       JOIN grids.{city}_settlements_{size}
                            USING (cell_id, year_model)
                       JOIN grids.{city}_urban_distance_{size} u1
                            USING (cell_id, year_model)
                       JOIN grids.{city}_urban_distance_{size} u2
                            USING (cell_id, year_model)
                       JOIN grids.{city}_urban_neighbours_{size}
                            USING (cell_id, year_model)
                       WHERE u1.built_threshold = {u1_built_threshold}
                       AND  u1.population_threshold = {u1_population_threshold}
                       AND u1.cluster_threshold = {u1_cluster_threshold}
                       AND u2.built_threshold = {u2_built_threshold}
                       AND u2.population_threshold = {u2_population_threshold}
                       AND u2.cluster_threshold = {u2_cluster_threshold}
                  """.format(city=city,
                             prefix=features_table_name,
                             size=grid_size,
                             selects=select_statement,
                             u1_built_threshold=urban_built_threshold,
                             u1_population_threshold=urban_population_threshold,
                             u1_cluster_threshold=urban_cluster_threshold,
                             u2_built_threshold=dense_built_threshold,
                             u3_population_threshold=dense_population_threshold,
                             u3_cluster_threshold=dense_cluster_threshold))

    INDEX = ("""CREATE INDEX ON features.{city}_{prefix}_{size} (year_model)"""
              .format(city=city,
                      prefix=features_table_name,
                      size=grid_size))
    db_conn = engine.raw_connection()
    cur = db_conn.cursor()
    cur.execute(QUERY_INSERT)
    cur.execute(INDEX)


def generate_labels(engine,
                    city,
                    labels_table_name,
                    years,
                    grid_size,
                    built_threshold,
                    population_threshold,
                    cluster_threshold):

    subqueries_list = []
    selects_list = []
    for i in range(len(years)-1):
        subqueries_list.append("""labels_{year} AS (
                                  SELECT cell_id,
                                         year_model,
                                         max(st_within(cell, geom)::int) as label
                            FROM grids.{city}_grids_{size}
                            LEFT JOIN grids.{city}_urban_clusters_{size}
                            ON st_within(cell, geom)
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
    INDEX = ("""CREATE INDEX ON features.{city}_{prefix}_{size} (year_model)"""
              .format(city=city,
                      prefix=labels_table_name,
                      size=grid_size))
    # get connection and send queries
    db_conn = engine.raw_connection()
    cur = db_conn.cursor()
    cur.execute(QUERY_LABELS)
    cur.execute(INDEX)





