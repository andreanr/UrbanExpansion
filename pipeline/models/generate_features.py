import pandas as pd
import json
import pdb

from . import utils

def generate_features(city, features, features_table_name, grid_size, intersect_percent):
    # select statement of features
    select_statement <- ", ".join(["COALESCE({x},0) AS {x}".format(x) for x in features])
    QUERY = ("""CREATE table features.{city}_{prefix}_{size}_{percent} AS (
                    SELECT cell_id,
                           year,
                           {selects}
                   FROM grids.{city}_airplanes_{size}
                   JOIN grids.{city}_highways_{size}
                        USING (cell_id)
                   JOIN grids.{city}_slope_{size}
                        USING (cell_id)
                   JOIN grids.{city}_urban_center_{size}
                        USING (cell_id)
                   JOIN grids.{city}_urban_distance_{size}
                        USING (cell_id)
                   JOIN grids.{city}_urban_neighbours_{size}
                        USING (cell_id, year)""".format(city=city,
                                                        prefix=features_table_name,
                                                        size=grid_size,
                                                        percent=intersect_percent))

    INDEX = ("""CREATE INDEX ON features.{city}_{prefix}_{size}_{percent} (year)"""
              .format(city=city,
                      prefix=features_table_name,
                      size=grid_size,
                      percent=intsersect_percent))
    engine = utils.get_engine()
    db_conn = engine.raw_connection()
    cur = db_conn.cursor()
    cur.execute(QUERY_INSERT)
    cur.execute(INDEX)


def generate_labels(city, labels_table_name, years, grid_size, intersect_percent):
    subqueries_list = []
    selects_list = []
    for i in range(len(years)-1):
        subqueries_list.append("""intersect_{year} AS (
                            SELECT cell_id,
                                   '{year}'::TEXT AS year
                                   sum(st_area(ST_Intersection(geom, cell_geom)) / st_area(cell_geom))
                                         AS porcentage_ageb_share
                            FROM grids.{city}_grid_{size}
                            LEFT JOIN preprocess.{next_year}
                            ON st_intersects(cell_geom, geom)
                            GROUP BY cell_id
                        ), labels_{year} AS (
                            SELECT cell_id,
                                   year,
                                   CASE WHEN  porcentage_ageb_share >= {percent} /100.0 
                                        THEN 1 ELSE 0 END AS label
                            FROM intersect_{year})""".format(year=years[i],
                                                            city=city,
                                                            size=grid_size,
                                                            next_year=years[i+1],
                                                            percent=intersect_percent))

        selects_list.append("""SELECT * FROM labels_{year}""".format(year=years[i]

    subqueries = ", ".join(subqueries_list)
    selects = " UNION ".join(selects_list)
    QUERY_LABELS = ("""CREATE TABLE features.{city}_{prefix}_{size}_{percent} AS (
                        WITH {subqueries}
                              {selects})""".format(city=city,
                                                   prefix=labels_table_name,
                                                   size=grid_size,
                                                   percent=intersect_percent,
                                                   subqueries=subqueries,
                                                   selects=selects))
    # Create index on years for labels
    INDEX = ("""CREATE INDEX ON features.{city}_{prefix}_{size}_{percent} (year)"""
              .format(city=city,
                      prefix=labels_table_name,
                      size=grid_size,
                      percent=intsersect_percent))
    # get connection and send queries
    engine = utils.get_engine()
    db_conn = engine.raw_connection()
    cur = db_conn.cursor()
    cur.execute(QUERY_LABELS)
    cur.execute(INDEX)





