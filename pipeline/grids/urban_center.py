

def urban_center_to_grid(grid_size, city, engine):
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
