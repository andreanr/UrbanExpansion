
def slope_to_grid(schema, city, engine):
    # Create table
    INSERT_CREATE = (""" WITH clip_slope_pct AS (
                           SELECT cell_id,
                                  ST_SummaryStats(st_union(st_clip(rast_percent, 1, cell, True))) AS stats_pct
                           FROM {schema}.{city}_grid
                           LEFT JOIN preprocess.slope
                           ON ST_Intersects(rast_percent, cell)
                           GROUP BY cell_id
                       ),
                       clip_slope_degrees AS (
                           SELECT cell_id,
                                   ST_SummaryStats(st_union(st_clip(rast_degrees, 1, cell, True))) AS stats_degrees
                           FROM {schema}.{city}_grid
                           LEFT JOIN preprocess.slope
                           ON ST_Intersects(rast_degrees, cell)
                           GROUP BY cell_id
                        ) INSERT INTO {schema}.{city}_slope
                            (cell_id,
                             min_slope_pct,
                             max_slope_pct,
                             mean_slope_pct,
                             stddev_slope_pct,
                             min_slope_degrees,
                             max_slope_degrees,
                             mean_slope_degrees,
                             stddev_slope_degrees)
                           SELECT cell_id,
                                   (stats_pct).min AS min_slope_pct,
                                   (stats_pct).max AS max_slope_pct,
                                   (stats_pct).mean AS mean_slope_pct,
                                   (stats_pct).stddev AS stddev_slope_pct,
                                   (stats_degrees).min AS min_slope_degrees,
                                   (stats_degrees).max AS max_slope_degrees,
                                   (stats_degrees).mean AS mean_slope_degrees,
                                   (stats_degrees).stddev AS stddev_slope_degrees
                           FROM clip_slope_pct
                           JOIN clip_slope_degrees
                           USING (cell_id)
                           )""".format(schema=schema,
                                       city=city))
    INDEX = ("""CREATE INDEX ON {schema}.{city}_slope (cell_id)"""
                .format(schema=schema,
                        city=city))

    db_conn = engine.raw_connection()
    cur = db_conn.cursor()
    cur.execute(QUERY_INSERT)
    cur.execute(INDEX)

