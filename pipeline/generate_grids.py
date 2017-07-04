import utils

def genhexagons_sql(city, esri, engine):
    db_conn = engine.raw_connection()
    cur = db_conn.cursor()

    # Drop query
    drop_func = ("""DROP FUNCTION genhexagons(float, float, float, float, float)""")
    cur.execute(drop_func)

    # Generate function in sql
    create_func = ("""CREATE OR REPLACE FUNCTION genhexagons(width float,
                                                             xmin float,
                                                             ymin float,
                                                             xmax float,
                                                             ymax float)
                    RETURNS float AS $total$
                    declare
                        b float :=width/2;
                        a float :=b/2; --sin(30)=.5
                        c float :=2*a;
                        height float := 2*a+c;  --1.1547*width;
                        ncol float :=ceil(abs(xmax-xmin)/width);
                        nrow float :=ceil(abs(ymax-ymin)/width);

                        polygon_string varchar := 'POLYGON((' ||
                                                            0 || ' ' || 0     || ' , ' ||
                                                            b || ' ' || a     || ' , ' ||
                                                            b || ' ' || a+c   || ' , ' ||
                                                            0 || ' ' || a+c+a || ' , ' ||
                                                         -1*b || ' ' || a+c   || ' , ' ||
                                                         -1*b || ' ' || a     || ' , ' ||
                                                            0 || ' ' || 0     ||
                                                    '))';
                    BEGIN
                        INSERT INTO public.grids_{city}_temp (cell) SELECT st_translate(cell, x_series*(2*a+c)+xmin, y_series*(2*(a +c))+ymin)
                        from generate_series(0, ncol::int , 1) as x_series,
                        generate_series(0, nrow::int,1 ) as y_series,
                        (
                           SELECT polygon_string::geometry as cell
                           UNION
                           SELECT ST_Translate(polygon_string::geometry, b , a+c)  as cell
                        ) as two_hex;
                        ALTER TABLE public.grids_{city}_temp
                        ALTER COLUMN cell TYPE geometry(Polygon, {esri})
                        USING ST_SetSRID(cell,{esri});
                        RETURN NULL;
                    END;
                    $total$ LANGUAGE plpgsql;""".format(city=city,
                                                        esri=esri))
    cur.execute(create_func)


def generate_grid(city, esri, grid_size, engine):
    db_conn = engine.raw_connection()
    cur = db_conn.cursor()
    # call sql functio
    genhexagons_sql(city, esri, engine)

    # temp table 
    query_tmp = ("""WITH buffer_prj AS (
                       SELECT st_transform(geom, {esri}) AS geom
                       FROM raw.{city}),
                       geom_bbox as (
                          SELECT
                               {grid_size} as width,
                               ST_XMin(geom) as xmin,
                               ST_YMin(geom) as ymin,
                               ST_XMax(geom) as xmax,
                               ST_YMax(geom) as ymax
                       FROM buffer_prj
                       GROUP BY geom)
                       SELECT genhexagons(width,xmin,ymin,xmax,ymax)
                       FROM geom_bbox;""".format(city=city,
                                                 esri=esri,
                                                 grid_size=grid_size))
    cur.execute(query_tmp)

    query_grid = ("""CREATE TABLE grids.{city}_grid_{grid_size} AS (
                        WITH buffer_prj AS (
                            SELECT st_transform(geom, {esri}) AS geom
                            FROM raw.{city})
                        SELECT grid.*
                        FROM public.grids_{city}_temp AS grid
                        JOIN buffer_prj
                        ON st_intersects(cell, geom)
                        );""".format(city=city,
                                     esri=esri,
                                     grid_size=grid_size))
    cur.execute(query_grid)
    drop_tmp = ("""DROP TABLE public.grids_{city}_temp""".format(city=city))
    cur.execute(drop_tmp)
