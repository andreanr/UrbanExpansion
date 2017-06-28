

def urban_center_to_grid(schema, city, engine):
    # insert query
    QUERY_INSERT = (""" INSERT INTO {schema}.{city}_urban_center
                        (cell_id, urban_center_distance_km)
                        SELECT cell_id,
                               ST_Distance(geom, st_centroid(cell)) /1000
                                       AS urban_center_distance_km
                        FROM {schema}.grid_{city}, preprocess.urban_center) """
                .format(schema=schema,
                        city=city))

    db_conn = engine.raw_connection()
    cur = db_conn.cursor()
    cur.execute(QUERY_INSERT)
