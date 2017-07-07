import utils

def water_bodies(grid_size, city, esri, engine):
    # insert query
    QUERY_INSERT = (""" INSERT INTO grids.{city}_water_bodies_{size}
                        (cell_id, water_bodies_distance_km, water_bodies_flag)
                        WITH wat_tr as (
                            SELECT st_transform(geom, {esri}) as geom
                            FROM raw.{city}_water_bodies)
                        SELECT cell_id,
                                min(ST_Distance(geom, st_centroid(cell))) / 1000
                                        AS water_bodies_distance_km,
                                CASE WHEN (min(ST_Distance(geom, st_centroid(cell))) / 1000.0)::float > 0
                                        THEN 0 ElSE 1 END as water_bodies_flag
                        FROM grids.{city}_grid_{size}, wat_tr
                        GROUP BY cell_id""".format(city=city,
                                                   size=grid_size,
                                                   esri=esri))
    db_conn = engine.raw_connection()
    cur = db_conn.cursor()
    cur.execute(QUERY_INSERT)
    db_conn.commit()
    db_conn.close()


def urban_center(grid_size, city, esri, engine):
    # insert query
    QUERY_INSERT = (""" INSERT INTO grids.{city}_urban_center_{size}
                        (cell_id, urban_center_distance_km)
			WITH center_tr as (
				SELECT st_transform(geom, {esri}) as geom
				FROM raw.{city}_urban_center)
                        SELECT cell_id,
                               ST_Distance(geom, st_centroid(cell)) /1000
                                       AS urban_center_distance_km
                        FROM grids.{city}_grid_{size}, center_tr """
                .format(size=grid_size,
                        city=city,
			esri=esri))

    db_conn = engine.raw_connection()
    cur = db_conn.cursor()
    cur.execute(QUERY_INSERT)
    db_conn.commit()
    db_conn.close()


def built_lds(grid_size, city, time, esri, engine):
    QUERY_INSERT = ("""WITH built_tr AS (
                            SELECT ST_DumpAsPolygons(st_transform(rast, {esri})) AS rast
                            FROM raw.{city}_built_lds_{time}
                    ) INSERT INTO grids.{city}_built_lds_{size}
                        (cell_id, year, min_built_lds, max_built_lds, mean_built_lds, stddev_built_lds, sum_built_lds)
                         SELECT cell_id,
                                {time} as year,
                                min((rast).val) AS min_built_lds,
                                max((rast).val) AS max_built_lds,
                                sum((rast).val) / count((rast)) AS mean_built_lds,
                                stddev((rast).val) AS stddev_built_lds,
                                sum((rast).val) AS sum_built_lds
                        FROM grids.{city}_grid_{size}
                        LEFT JOIN built_tr 
                        ON ST_intersects(cell, (rast).geom)
                        GROUP BY cell_id""".format(city=city,
                                                  size=grid_size,
                                                  time=time,
                                                  esri=esri))
    db_conn = engine.raw_connection()
    cur = db_conn.cursor()
    cur.execute(QUERY_INSERT)
    db_conn.commit()
    db_conn.close()


def settlements(grid_size, city, time, esri, engine):
    QUERY_INSERT = ("""WITH settlements_tr AS (
                            SELECT ST_DumpAsPolygons(st_transform(rast, {esri})) AS rast
                            FROM raw.{city}_settlements_{time}
                    ) INSERT INTO grids.{city}_settlements_{size}
                        (cell_id, year, min_settlements, max_settlements, mean_settlements, sum_settlements)
                         SELECT cell_id,
                                {time} as year,
                                min((rast).val) AS min_settlements,
                                max((rast).val) AS max_settlements,
                                sum((rast).val) / count((rast)) AS mean_settlements,
                                sum((rast).val) AS sum_settlements
                        FROM grids.{city}_grid_{size} 
                        LEFT JOIN settlements_tr 
                        ON ST_intersects(cell, (rast).geom) 
                        GROUP BY cell_id""".format(city=city,
                                                  size=grid_size,
                                                  time=time,
                                                  esri=esri))
    db_conn = engine.raw_connection()
    cur = db_conn.cursor()
    cur.execute(QUERY_INSERT)
    db_conn.commit()
    db_conn.close()


## TODO
def urban_distance(grid_size, city, time, esri, engine):
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


## TODO
def urban_neighbours(city, time, grid_size, esri, intersect_percent, engine):
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


def population(grid_size, city, time, esri, engine):
    QUERY_INSERT = (""" WITH pop_tr AS (
                            SELECT ST_DumpAsPolygons(st_transform(rast, {esri})) as rast
                            FROM raw.{city}_population_{time}
                        ) INSERT INTO grids.{city}_population_{size}
                          (cell_id, year, min_population, max_population, mean_population, stddev_population, sum_population)
                            SELECT cell_id,
                                   {time} as year,
                                   min((rast).val) AS min_population,
                                   max((rast).val) AS max_population,
                                   sum((rast).val) / count((rast)) AS mean_population,
                                   stddev((rast).val) AS stddev_population,
                                   sum((rast).val) AS sum_population
                            FROM grids.{city}_grid_{size}
                            LEFT JOIN pop_tr
                            ON ST_intersects(cell, (rast).geom)
                            GROUP BY cell_id""".format(city=city,
                                                    size=grid_size,
                                                    esri=esri,
                                                    time=time))

    db_conn = engine.raw_connection()
    cur = db_conn.cursor()
    cur.execute(QUERY_INSERT)
    db_conn.commit()
    db_conn.close()


def dem(grid_size, city, esri, engine):
    QUERY_INSERT = (""" WITH dem_tr AS (
                            SELECT ST_DumpAsPolygons(st_transform(rast, {esri})) as rast
                            FROM raw.{city}_dem
                        ) INSERT INTO grids.{city}_dem_{size}
                          (cell_id, min_dem, max_dem, mean_dem, stddev_dem)
                            SELECT cell_id,
                                   min((rast).val) AS min_dem,
                                   max((rast).val) AS max_dem,
                                   sum((rast).val) / count((rast)) AS mean_dem,
                                   stddev((rast).val) AS stddev_dem
                            FROM grids.{city}_grid_{size}
                            LEFT JOIN dem_tr
                            ON ST_intersects(cell, (rast).geom)
                            GROUP BY cell_id""".format(city=city,
                                                    size=grid_size,
                                                    esri=esri))
    db_conn = engine.raw_connection()
    cur = db_conn.cursor()
    cur.execute(QUERY_INSERT)
    db_conn.commit()
    db_conn.close()


def slope(grid_size, city, esri, engine):
    # Create table
    QUERY_INSERT = ("""WITH slope_pct AS (
                           SELECT ST_DumpAsPolygons(rast_percent) AS rast_pct
                           FROM raw.{city}_slope
                      ), slope_degrees AS (
                           SELECT ST_DumpAsPolygons(rast_degrees) AS rast_degrees
                           FROM raw.{city}_slope
                      ), slope_cell_pct AS (
                           SELECT cell_id,
                                  min((rast_pct).val) AS min_slope_pct,
                                  max((rast_pct).val) AS max_slope_pct,
                                  sum((rast_pct).val) / count((rast_pct)) AS mean_slope_pct,
                                  stddev((rast_pct).val) AS stddev_slope_pct
                           FROM grids.{city}_grid_{size}
                           LEFT JOIN slope_pct
                           ON St_intersects(cell, (rast_pct).geom)
                           GROUP BY cell_id
                       ), slope_cell_degrees AS (
                            SELECT cell_id,
                                   min((rast_degrees).val) AS min_slope_degrees,
                                   max((rast_degrees).val) AS max_slope_degrees,
                                   sum((rast_degrees).val) / count((rast_degrees)) AS mean_slope_degrees,
                                   stddev((rast_degrees).val) AS stddev_slope_degrees
                             FROM grids.{city}_grid_{size}
                             LEFT JOIN slope_degrees
                             ON St_intersects(cell, (rast_degrees).geom)
                             GROUP BY cell_id
                        ) INSERT INTO grids.{city}_slope_{size}
                            (cell_id,
                             min_slope_pct,
                             max_slope_pct,
                             mean_slope_pct,
                             stddev_slope_pct,
                             min_slope_degrees,
                             max_slope_degrees,
                             mean_slope_degrees,
                             stddev_slope_degrees)
                          SELECT * 
                          FROM slope_cell_pct
                          JOIN slope_cell_degrees
                          USING (cell_id)
                           """.format(size=grid_size,
                                       city=city,
                                       esri=esri))

    db_conn = engine.raw_connection()
    cur = db_conn.cursor()
    cur.execute(QUERY_INSERT)
    db_conn.commit()
    db_conn.close()

def city_lights(grid_size, city, time, esri, engine):
    QUERY_INSERT = ("""WITH lights_tr AS (
                            SELECT ST_DumpAsPolygons(st_transform(rast, {esri})) AS rast
                            FROM raw.{city}_city_lights_{time}
                    ) INSERT INTO grids.{city}_city_lights_{size}
                        (cell_id, year, min_city_lights, max_city_lights, mean_city_lights, stddev_city_lights, sum_city_lights)
                         SELECT cell_id,
                                {time} as year,
                                min((rast).val) AS min_city_lights,
                                max((rast).val) AS max_city_lights,
                                sum((rast).val) / count((rast)) AS mean_city_lights,
                                stddev((rast).val) AS stddev_city_lights,
                                sum((rast).val) AS sum_city_lights
                        FROM grids.{city}_grid_{size}
                        LEFT JOIN lights_tr
                        ON ST_intersects(cell, (rast).geom)
                        GROUP BY cell_id""".format(city=city,
                                                  size=grid_size,
                                                  time=time,
                                                  esri=esri))
    db_conn = engine.raw_connection()
    cur = db_conn.cursor()
    cur.execute(QUERY_INSERT)
    db_conn.commit()
    db_conn.close()

if __name__ == "__main__":
    grid_size = 250
    city = 'amman'
    esri = 32236
    time = 2013
    engine = utils.get_engine()
    #print('water bodies')
    #water_bodies(grid_size, city, esri, engine)
    #print('urban_center')
    #urban_center(grid_size, city, esri, engine)
    print('slope')
    slope(grid_size, city, esri, engine)
    #print('built_lds')
    #built_lds(grid_size, city, time, esri, engine)
    #print('dem')
    #dem(grid_size, city, esri, engine)
    #print('popuation')
    #population(grid_size, city, time, esri, engine)
    #print('city lights')
    #city_lights(grid_size, city, time, esri, engine)
    #print('settlements')
    #settlements(grid_size, city, time, esri, engine)
