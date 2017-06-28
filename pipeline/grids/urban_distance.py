

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
