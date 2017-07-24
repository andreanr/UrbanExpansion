import utils

def water_bodies(grid_size, city, esri):
    # insert query
    QUERY_INSERT = (""" INSERT INTO grids.{city}_water_bodies_{size}
                        (cell_id, water_bodies_distance_km, water_bodies_flag)
                        WITH wat_tr as (
                            SELECT st_transform(geom, {esri}) as geom
                            FROM raw.{city}_water_bodies)
                        SELECT cell_id,
                                min(ST_Distance(geom, st_centroid(cell))) / 1000.0
                                        AS water_bodies_distance_km,
                                CASE WHEN (min(ST_Distance(geom, st_centroid(cell))) / 1000.0)::float > 0
                                        THEN 0 ElSE 1 END as water_bodies_flag
                        FROM grids.{city}_grid_{size}, wat_tr
                        GROUP BY cell_id""".format(city=city,
                                                   size=grid_size,
                                                   esri=esri))
    return QUERY_INSERT

def highways(grid_size, city, esri):
    # insert query
    QUERY_INSERT = (""" INSERT INTO grids.{city}_highways_{size}
                        (cell_id, distance_highways_km)
                        WITH highways_tr as (
                            SELECT st_transform(geom, {esri}) AS geom
                            FROM raw.{city}_highways )
                        SELECT cell_id,
                               min(ST_Distance(geom, st_centroid(cell))) / 1000.0
                               AS distance_highways_km
                        FROM grids.{city}_grid_{size}, highways_tr
                        GROUP BY cell_id""".format(city=city,
                                                   size=grid_size,
                                                   esri=esri))
    return QUERY_INSERT


def geopins(grid_size, city, esri):
    QUERY_INSERT = (""" INSERT INTO grids.{city}_geopins_{size}
                       (cell_id, worship_distance, school_distance,
                        university_distance, hospital_distance, aeroway_distance)
                      WITH geopins_tr as (
                           SELECT amenity, aeroway, st_transform(geom, {esri}) as geom
                           FROM raw.{city}_geopins )
                      SELECT cell_id,
                             min(CASE WHEN amenity IN ('place of worship')
                                   THEN st_distance(st_centroid(cell), ST_ClosestPoint(geom, st_centroid(cell))) / 1000.0
                                  ELSE NULL END) AS worship_distance,
                             min(CASE WHEN  amenity  IN ('school')
                                     THEN st_distance(st_centroid(cell), ST_ClosestPoint(geom, st_centroid(cell))) / 1000.0
                                     ELSE NULL END) AS school_distance,
                             min(CASE WHEN amenity  IN ('university')
                                     THEN st_distance(st_centroid(cell), ST_ClosestPoint(geom, st_centroid(cell))) / 1000.0
                                     ELSE NULL END) AS university_distance,
                             min(CASE WHEN amenity  IN ('hospital')
                                     THEN st_distance(st_centroid(cell), ST_ClosestPoint(geom, st_centroid(cell))) / 1000.0
                                     ELSE NULL END) AS hospital_distance,
                             min(CASE WHEN  aeroway IS NOT NULL
                                     THEN st_distance(st_centroid(cell), ST_ClosestPoint(geom, st_centroid(cell))) / 1000.0
                                     ELSE NULL END) AS aeroway_distance
                     FROM grids.{city}_grid_{size}, geopins_tr
                     GROUP BY cell_id""".format(city=city,
                                                size=grid_size,
                                                esri=esri))
    return QUERY_INSERT

def city_center(grid_size, city, esri):
    # insert query
    QUERY_INSERT = (""" INSERT INTO grids.{city}_city_center_{size}
                        (cell_id, city_center_distance_km)
			WITH center_tr as (
				SELECT st_transform(geom, {esri}) as geom
				FROM raw.{city}_city_center)
                        SELECT cell_id,
                               ST_Distance(geom, st_centroid(cell)) /1000
                                       AS city_center_distance_km
                        FROM grids.{city}_grid_{size}, center_tr """
                .format(size=grid_size,
                        city=city,
			esri=esri))

    return QUERY_INSERT

def built_lds(grid_size, city, esri, time, year_model):
    QUERY_INSERT = ("""WITH built_tr AS (
                            SELECT ST_DumpAsPolygons(st_transform(rast, {esri})) AS rast
                            FROM raw.{city}_built_lds_{time}
                    ) INSERT INTO grids.{city}_built_lds_{size}
                        (cell_id, year, year_model, min_built_lds, max_built_lds, mean_built_lds, stddev_built_lds, sum_built_lds)
                         SELECT cell_id,
                                {time} as year,
                                {year_model} as year_model,
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
                                                  year_model=year_model,
                                                  esri=esri))
    return QUERY_INSERT

def settlements(grid_size, city, esri, time, year_model):
    QUERY_INSERT = ("""WITH settlements_tr AS (
                            SELECT ST_DumpAsPolygons(st_transform(rast, {esri})) AS rast
                            FROM raw.{city}_settlements_{time}
                    ) INSERT INTO grids.{city}_settlements_{size}
                        (cell_id, year, year_model,  min_settlements, max_settlements, mean_settlements, sum_settlements)
                         SELECT cell_id,
                                {time} as year,
                                {year_model} as year_model,
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
                                                  year_model=year_model,
                                                  esri=esri))
    return QUERY_INSERT

def urban_clusters(grid_size, city, built_threshold, population_threshold, year_model):
    QUERY_INSERT = (""" WITH urban AS (SELECT 
                            (ST_Dump(st_union(cell))).geom AS cell_cluster
                             FROM grids.{city}_built_lds_{size}
                              JOIN grids.{city}_population_{size}
                              USING (cell_id, year_model)
                              JOIN grids.{city}_grid_{size}
                              USING (cell_id)
                              WHERE year_model = {year_model}
                              AND max_built_lds > {built_threshold} / 100.00
                              AND max_population >= {population_threshold}
                        ), urban_population AS ( 
                            SELECT cell_cluster,
                                  sum(mean_population) AS population
                            FROM grids.{city}_population_{size}
                            JOIN grids.{city}_grid_{size}
                            USING (cell_id)
                            JOIN urban
                            ON st_within(cell, cell_cluster)
                            GROUP BY cell_cluster)
                        INSERT INTO grids.{city}_urban_clusters_{size}
                        (cluster_id, year_model, population, built_threshold, population_threshold, geom)
                        SELECT ROW_NUMBER() OVER (ORDER BY cell_cluster) AS cluster_id,
                        {year_model} as year_model,
                        population,
                        {built_threshold} as built_threshold,
                        {population_threshold} as population_threshold,
                        cell_cluster as geom
                        FROM urban_population
                        """.format(city=city,
                                   size=grid_size,
                                   built_threshold=built_threshold,
                                   population_threshold=population_threshold,
                                   year_model=year_model))

    return QUERY_INSERT


def urban_distance(grid_size, city, built_threshold, population_threshold, cluster_threshold, 
                           year_model):
    QUERY_INSERT = ("""INSERT INTO grids.{city}_urban_distance_{size}
               (cell_id, year_model, built_threshold, population_threshold, cluster_threshold,
                    urban_flag,  urban_distance_km)
               SELECT cell_id,
                      {year_model} as year_model,
                      {built_threshold},
                      {population_threshold},
                      {cluster_threshold},
                      CASE WHEN (min(ST_Distance(geom, st_centroid(cell))) / 1000.0)::float > 0
                        THEN 0 else 1 end as urban_flag,
                      min(st_distance(geom, st_centroid(cell))) / 1000.0
                        AS urban_distance_km
                FROM grids.{city}_grid_{size}, grids.{city}_urban_clusters_{size}
                WHERE built_threshold = {built_threshold}
                AND year_model = {year_model}
                AND population_threshold = {population_threshold}
                AND population >= {cluster_threshold}
                GROUP BY cell_id""".format(size=grid_size,
                                            city=city,
                                            built_threshold=built_threshold,
                                            population_threshold=population_threshold,
                                            cluster_threshold=cluster_threshold,
                                            year_model=year_model))
    return QUERY_INSERT

## TODO
def urban_neighbours(grid_size, city, built_threshold, population_threshold, cluster_threshold,
                           year_model):
    QUERY_INSERT = (""" WITH urban_temp AS (
                             SELECT cell_id,
                                    CASE WHEN ST_within(cell, geom) 
                                       THEN 1 else 0 end as urban_flag,
                                   cell
                             FROM grids.{city}_grid_{size}, grids.{city}_urban_clusters_{size}
                             WHERE built_threshold = {built_threshold}
                             AND year_model = {year_model}
                             AND population_threshold = {population_threshold}
                             AND population >= {cluster_threshold}
                      ), vecinos AS (
                             SELECT  cell_id,
                                     cell,
                                     ARRAY(SELECT p.urban_flag
                                           FROM urban_temp p
                                           WHERE ST_Touches(b.cell, p.cell)
                                           AND b.cell_id <> p.cell_id) AS urban_neighbours
                            FROM grids.{city}_grid_{size} b
                      )
                      INSERT INTO grids.{city}_urban_neighbours_{size}
                      (cell_id, year_model, built_threshold, population_threshold,
                       cluster_threshold, urban_neighbours) 
                      SELECT cell_id,
                               {year_model} as year_model,
                               {built_threshold} AS built_threshold,
                               {population_threshold} AS population_threshold,
                               {cluster_threshold} AS cluster_threshold,     
                               (SELECT SUM(s) FROM UNNEST(urban_neighbours) s)
                      FROM vecinos""".format(city=city,
                                             size=grid_size,
                                             year_model=year_model,
                                             built_threshold=built_threshold,
                                             population_threshold= population_threshold,
                                             cluster_threshold=cluster_threshold))
    return QUERY_INSERT


def population(grid_size, city, esri, time, year_model):
    QUERY_INSERT = (""" WITH pop_tr AS (
                            SELECT ST_DumpAsPolygons(st_transform(rast, {esri})) as rast
                            FROM raw.{city}_population_{time}
                        ) INSERT INTO grids.{city}_population_{size}
                          (cell_id, year, year_model, min_population, max_population, mean_population, stddev_population, sum_population)
                            SELECT cell_id,
                                   {time} as year,
                                    {year_model} as year_model,
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
                                                    year_model=year_model,
                                                    time=time))

    return QUERY_INSERT

def dem(grid_size, city, esri):
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
    return QUERY_INSERT

def slope(grid_size, city, esri):
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
    return QUERY_INSERT


def city_lights(grid_size, city, esri, time, year_model):
    QUERY_INSERT = ("""WITH lights_tr AS (
                            SELECT ST_DumpAsPolygons(st_transform(rast, {esri})) AS rast
                            FROM raw.{city}_city_lights_{time}
                    ) INSERT INTO grids.{city}_city_lights_{size}
                        (cell_id, year, year_model, min_city_lights, max_city_lights, mean_city_lights, stddev_city_lights, sum_city_lights)
                         SELECT cell_id,
                                {time} as year,
                                {year_model} as year_model,
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
                                                  year_model=year_model,
                                                  esri=esri))
    return QUERY_INSERT

if __name__ == "__main__":
    grid_size = 250
    city = 'amman'
    esri = 32236
    time = 1990
    built_threshold = 50
    population_threshold = 75
    cluster_threshold = 5000
    year_model = 1990
    engine = utils.get_engine()
    #print('water bodies')
    #water_bodies(grid_size, city, esri, engine)
    print(geopins)
    geopins(grid_size, city, esri, engine)
    #print('urban_center')
    #urban_center(grid_size, city, esri, engine)
    #print('slope')
    #slope(grid_size, city, esri, engine)
    #print('highways')
    #highways(grid_size, city, esri, engine)
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
    #print('urban cluster')
    #urban_clusters(grid_size, city, built_threshold, population_threshold, year_model, engine)
    #print('built_distance')
    #built_distance(grid_size, city, built_threshold, time, year_model, esri, engine)
    #print('urban distance')
    #urban_distance(grid_size, city, built_threshold, population_threshold, cluster_threshold, year_model, engine) 
    #print('urban neighbours')
    #urban_neighbours(grid_size, city, built_threshold, population_threshold, cluster_threshold, year_model, engine)
