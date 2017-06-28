

def urban_neighbours(schema, city, time, intersect_percent, engine):
    SUBQUERY = ("""WITH intersect_{time} AS (
                    SELECT  cell_id,
                            cell_geom,
                            sum(st_area(ST_Intersection(geom, cell_geom)) / st_area(cell_geom)) as porcentage_ageb_share
                FROM {schema}.{city}_grid
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
                FROM {schema}.{city}_grid b
            )""".format(schema=schema,
                        city=city,
                        time=time,
                        percent=intersect_percent))

    INSERT_QUERY = (""" INSERT INTO {schema}.{city}_urban_neighbours
                        (cell_id,
                         year,
                         intersect_percent,
                         urban_neighbours)
                    SELECT cell_id,
                          '{time}'::TEXT as year,
                           {percent} as intersect_percent,
                          (SELECT SUM(s) FROM UNNEST(urban_neighbours) s)
                    FROM vecinos_{time}""".format(schema=schema,
                                                  city=city,
                                                  time=time,
                                                  percent=intersect_percent))

    QUERY = SUBQUERY + INSERT_QUERY
    db_conn = engine.raw_connection()
    cur = db_conn.cursor()
    cur.execute(QUERY)
