

def urban_center(grid_size, city, engine):
    # insert query
    QUERY_INSERT = (""" INSERT INTO grids.{city}_urban_center_{size}
                        (cell_id, urban_center_distance_km)
                        SELECT cell_id,
                               ST_Distance(geom, st_centroid(cell)) /1000
                                       AS urban_center_distance_km
                        FROM grids.{city}_grid_{size}, preprocess.urban_center) """
                .format(size=grid_size,
                        city=city))

    db_conn = engine.raw_connection()
    cur = db_conn.cursor()
    cur.execute(QUERY_INSERT)


def urban_distance(grid_size, city, time, engine):
    QUERY = (""" INSERT INTO grids.{city}_urban_distance_{size}
                (cell_id,
                 year,
                urban_distance_km)
                SELECT cell_id,
                       '{time}' AS "year",
                      min(st_distance(geom, st_centroid(cell_geom))) / 1000.0
                         AS urban_distance_min
                FROM preprocess.{city}_urban_{time}, {schema}.{city}_grid
                GROUP BY cell_id""".format(size=grid_size,
                                           city=city,
                                           time=time))
    db_conn = engine.raw_connection()
    cur = db_conn.cursor()
    cur.execute(QUERY_INSERT)


def urban_neighbours(city, time, grid_size, intersect_percent, engine):
    SUBQUERY = ("""WITH intersect_{time} AS (
                    SELECT  cell_id,
                            cell_geom,
                            sum(st_area(ST_Intersection(geom, cell_geom)) / st_area(cell_geom)) as porcentage_ageb_share
                FROM grids.{city}_grid_{size}
                LEFT JOIN preprocess.urban_{city}_{time}
                ON st_intersects(cell_geom, geom)
                 GROUP BY cell_id
            ),zu_{time} AS (
                SELECT  cell_id,
                        cell_geom,
                        CASE WHEN  porcentage_ageb_share >= {percent}/100.0
                                    THEN 1 ELSE 0 END AS urbano
                FROM intersect_{time}
            ), vecinos_{time} AS (
                SELECT  cell_id,
                        cell_geom,
                        ARRAY(SELECT p.urbano
                              FROM zu_{time} p
                       WHERE ST_Touches(b.cell_geom, p.cell_geom)
                       AND b.cell_id <> p.cell_id) AS urban_neighbours
                FROM grids.{city}_grid_{size} b
            )""".format(size=grid_size,
                        city=city,
                        time=time,
                        percent=intersect_percent))

    INSERT_QUERY = (""" INSERT INTO grids.{city}_urban_neighbours_{size}
                        (cell_id,
                         year,
                         intersect_percent,
                         urban_neighbours)
                    SELECT cell_id,
                          '{time}'::TEXT as year,
                           {percent} as intersect_percent,
                          (SELECT SUM(s) FROM UNNEST(urban_neighbours) s)
                    FROM vecinos_{time}""".format(size=grid_size,
                                                  city=city,
                                                  time=time,
                                                  percent=intersect_percent))

    QUERY = SUBQUERY + INSERT_QUERY
    db_conn = engine.raw_connection()
    cur = db_conn.cursor()
    cur.execute(QUERY)

def slope(schema, city, engine):
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

    db_conn = engine.raw_connection()
    cur = db_conn.cursor()
    cur.execute(QUERY_INSERT)
