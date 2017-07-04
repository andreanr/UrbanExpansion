import utils
import pdb

def genhexagons_sql(city, esri, engine):
    db_conn = engine.raw_connection()
    cur = db_conn.cursor()


    drop_table = ("""DROP TABLE IF EXISTS public.grids_{city}_temp""".format(city=city))
    cur.execute(drop_table)
    db_conn.commit()

    create_table = ("""CREATE  TABLE public.grids_{city}_temp (cell_id serial not null primary key)"""
                    .format(city=city))
    cur.execute(create_table)
    db_conn.commit()

    add_geom = ("""SELECT addgeometrycolumn('public', 'grids_{city}_temp','cell', 0, 'POLYGON', 2)"""
                .format(city=city))
    cur.execute(add_geom)
    db_conn.commit()

    # Drop query
    drop_func = ("""DROP FUNCTION genhexagons(float, float, float, float, float)""")
    cur.execute(drop_func)
    db_conn.commit()

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
                        USING ST_SetSRID(cell, {esri});
                        RETURN NULL;
                    END;
                    $total$ LANGUAGE plpgsql;""".format(city=city,
                                                        esri=esri))
    cur.execute(create_func)
    db_conn.commit()
    db_conn.close()


def generate_grid(city, esri, grid_size, engine):
    db_conn = engine.raw_connection()
    cur = db_conn.cursor()
    # call sql functio
    genhexagons_sql(city, esri, engine)

    # temp table 
    query_tmp = ("""WITH buffer_prj AS (
                         SELECT st_transform(geom, {esri}) as geom
                         FROM raw.{city}
                    ), geom_bbox as (
                          SELECT
                               {grid_size} as width,
                               ST_XMin(geom) as xmin,
                               ST_YMin(geom) as ymin,
                               ST_XMax(geom) as xmax,
                               ST_YMax(geom) as ymax
                       FROM buffer_prj
                       GROUP BY geom)
                       SELECT genhexagons(width,xmin,ymin,xmax,ymax)
                       FROM geom_bbox""".format(city=city,
                                                 esri=esri,
                                                 grid_size=grid_size))
    cur.execute(query_tmp)
    db_conn.commit()

    drop_grid = ("""DROP TABLE grids.{city}_grid_{size} CASCADE""".format(city=city,
                                                                 size=grid_size))
    cur.execute(drop_grid)
    db_conn.commit()

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
    db_conn.commit()
    ALTER = ("""ALTER TABLE grids.{city}_grid_{size}
                    ALTER COLUMN cell TYPE geometry(Polygon, {esri})
                    USING ST_SetSRID(cell,{esri})""".format(city=city,
                                                            size=grid_size,
                                                            esri=esri))
    cur.execute(ALTER)
    db_conn.commit()

    # add index
    p_key = ("""ALTER TABLE grids.{city}_grid_{size} ADD PRIMARY KEY (cell_id)"""
             .format(city=city,
                     size=grid_size))
    cur.execute(p_key)
    db_conn.commit()

    drop_tmp = ("""DROP TABLE public.grids_{city}_temp""".format(city=city))
    cur.execute(drop_tmp)
    db_conn.commit()
    db_conn.close()


def generate_table(engine, city, grid_size, name, columns_dict):
    db_conn = engine.raw_connection()
    cur = db_conn.cursor()
    DROP_QUERY = ("""DROP TABLE IF EXISTS grids.{city}_{name}_{size}"""
                  .format(city=city,
                          name=name,
                          size=grid_size))
    cur.execute(DROP_QUERY)
    db_conn.commit()

    columns_ref = []
    for col, ref in columns_dict.items():
        columns_ref.append(""" {col}   {ref} """.format(col=col,
                                                        ref=ref))
    columns_ref = ", ".join(columns_ref)
    QUERY = ("""CREATE TABLE grids.{city}_{name}_{size} (
                    cell_id  INT REFERENCES grids.{city}_grid_{size} (cell_id),
                    {columns_ref}
                )""".format(city=city,
                            size=grid_size,
                            name=name,
                            columns_ref=columns_ref))
    cur.execute(QUERY)
    db_conn.commit()
    db_conn.close()

if __name__ == "__main__":
    yaml_file = 'grid_tables.yaml'
    city = 'amman'
    grid_size = 1000
    esri = 32236
    tables = utils.read_yaml(yaml_file)
    # connection
    engine = utils.get_engine()
    generate_grid(city, esri, grid_size, engine)

    for table, cols in tables.items():
        q = generate_table(engine, city, grid_size, table, cols)
