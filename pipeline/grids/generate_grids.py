import luigi
import pdb
from luigi.contrib import postgres
import utils
from commons import city_task
from etl.preprocess import PreprocessTask

class CreateTmpTable(city_task.PostgresTask):
    """
    Task that creates a table (tmp) that will be used
    for generating the grid. This task:
        - drops tmp table if exists
        - creates table
        - adds geometry column
    """
    city = luigi.Parameter()
    grid_size = luigi.Parameter()

    def requires(self):
        return PreprocessTask()

    @property
    def table(self):
        return """public.grids_{city}_{size}_temp""".format(city=self.city,
                                                            size=self.grid_size)

    @property
    def query(self):
        drop = ("""DROP TABLE IF EXISTS public.grids_{city}_{size}_temp;"""
            .format(city=self.city, size=self.grid_size))
        create = ("""CREATE TABLE public.grids_{city}_{size}_temp
                        (cell_id serial not null primary key);"""
                    .format(city=self.city, size=self.grid_size))
        add_geom = ("""SELECT addgeometrycolumn('public', 'grids_{city}_{size}_temp','cell', 0, 'POLYGON', 2);"""
                     .format(city=self.city, size=self.grid_size))
        return drop + create + add_geom


class HexagonsSQL(city_task.PostgresTask):
    """
    Task that generates the stored procedure
    that generates the hexagon grid into the
    tmp table given the:
    Args:
        city (str): city name. Will use the city buffer
                    bbox for construction area.
        grid_size (int): size of the grid in meters.
                   It is measure as twice the apothem
        esri (int): spatial reference code
    """
    city = luigi.Parameter()
    grid_size = luigi.Parameter()
    esri = luigi.Parameter()

    def requires(self):
        return CreateTmpTable(self.city, self.grid_size)

    @property
    def table(self):
        return """public.grids_{city}_{size}_temp""".format(city=self.city,
                                                            size=self.grid_size)

    @property
    def query(self):
        drop = ("""DROP FUNCTION genhexagons(float, float, float, float, float);""")
        create =  ("""CREATE OR REPLACE FUNCTION genhexagons(side_length float,
                                                             xmin float,
                                                             ymin float,
                                                             xmax float,
                                                             ymax float)
                    RETURNS float AS $total$
                    declare
                        c float :=side_length;
                        a float :=c/2;
                        b float :=c * sqrt(3)/2;
                        height float := 2*a+c;
                        ncol float :=ceil(abs(xmax-xmin)/(2*b));
                        nrow float :=ceil(abs(ymax-ymin)/(2*c));

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
                        INSERT INTO public.grids_{city}_{size}_temp (cell) SELECT st_translate(cell, x_series*(2*b)+xmin, y_series*(2*(a +c))+ymin)
                        from generate_series(0, ncol::int , 1) as x_series,
                        generate_series(0, nrow::int,1 ) as y_series,
                        (
                           SELECT polygon_string::geometry as cell
                           UNION
                           SELECT ST_Translate(polygon_string::geometry, b , a+c)  as cell
                        ) as two_hex;
                        ALTER TABLE public.grids_{city}_{size}_temp
                        ALTER COLUMN cell TYPE geometry(Polygon, {esri})
                        USING ST_SetSRID(cell, {esri});
                        RETURN NULL;
                    END;
                    $total$ LANGUAGE plpgsql;""".format(city=self.city,
                                                        size=self.grid_size,
                                                        esri=self.esri))
        return drop + create


class GenerateGrid(city_task.PostgresTask):
    """
    Task that calls the stored procedure
    and cuts the grid to the buffer city area
    Stores the grid on grids.{city}_grid_{grid_size}
    """
    city = luigi.Parameter()
    grid_size = luigi.Parameter()
    esri = luigi.Parameter()

    def requires(self):
        return HexagonsSQL(self.city,
                           self.grid_size,
                           self.esri)

    @property
    def table(self):
        return """grids.{city}_grid_{size}""".format(city=self.city,
                                                     size=self.grid_size)

    @property
    def query(self):
        tmp =  ("""WITH buffer_prj AS (
                         SELECT st_transform(geom, {esri}) as geom
                         FROM raw.{city}
                    ), geom_bbox as (
                          SELECT
                               {grid_size} as side_length,
                               ST_XMin(geom) as xmin,
                               ST_YMin(geom) as ymin,
                               ST_XMax(geom) as xmax,
                               ST_YMax(geom) as ymax
                       FROM buffer_prj
                       GROUP BY geom)
                       SELECT genhexagons(side_length,xmin,ymin,xmax,ymax)
                       FROM geom_bbox;""".format(city=self.city,
                                                 esri=self.esri,
                                                 grid_size=self.grid_size))

        create = ("""CREATE TABLE grids.{city}_grid_{grid_size} AS (
                        WITH buffer_prj AS (
                            SELECT st_transform(geom, {esri}) AS geom
                            FROM raw.{city})
                        SELECT grid.*
                        FROM public.grids_{city}_{grid_size}_temp AS grid
                        JOIN buffer_prj
                        ON st_intersects(cell, geom)
                        );""".format(city=self.city,
                                     esri=self.esri,
                                     grid_size=self.grid_size))

        drop_tmp = ("""DROP TABLE IF EXISts public.grids_{city}_{grid_size}_temp;"""
                    .format(city=self.city,
                            grid_size=self.grid_size))

        geom = ("""ALTER TABLE grids.{city}_grid_{size}
                    ALTER COLUMN cell TYPE geometry(Polygon, {esri})
                    USING ST_SetSRID(cell,{esri});""".format(city=self.city,
                                                            size=self.grid_size,
                                                            esri=self.esri))

        primary_key =  ("""ALTER TABLE grids.{city}_grid_{size} ADD PRIMARY KEY (cell_id);"""
                        .format(city=self.city,
                                size=self.grid_size))

        return tmp + create + drop_tmp + geom + primary_key


class GenerateTable(city_task.PostgresTask):
    """
    Task that generates an empty table of grids features
    given:
    Args:
        city (str): name of the city
        grid_size (int/str): grid size in meters
        name (string): feature name
        columns_dict (dict): dictionary of names of columns
                        and their type
    """
    city = luigi.Parameter()
    grid_size = luigi.Parameter()
    name = luigi.Parameter()
    columns_dict = luigi.DictParameter()

    def requires(self):
        return PreprocessTask()

    @property
    def table(self):
        return """grids.{city}_{name}_{size}""".format(city=self.city,
                                                       name=self.name,
                                                       size=self.grid_size)
    @property
    def query(self):
        columns_ref = []
        for col, ref in self.columns_dict.items():
            columns_ref.append(""" {col}   {ref} """.format(col=col,
                                                            ref=ref))
        columns_ref = ", ".join(columns_ref)
        DROP = ("""DROP TABLE IF EXISTS grids.{city}_{name}_{size};"""
                .format(city=self.city,
                        size=self.grid_size,
                        name=self.name))
        QUERY = ("""CREATE TABLE grids.{city}_{name}_{size} (
                    cell_id  INT REFERENCES grids.{city}_grid_{size} (cell_id),
                    {columns_ref}
                );""".format(city=self.city,
                            size=self.grid_size,
                            name=self.name,
                            columns_ref=columns_ref))
        return DROP + QUERY


class GenerateUrbanClusterTable(city_task.PostgresTask):
    """
    Generates empty urban clusters tables on grid schema
    """
    city = luigi.Parameter()
    grid_size = luigi.Parameter()

    def requires(self):
        return PreprocessTask()

    @property
    def table(self):
        return """grids.{city}_urban_clusters_{size}""".format(city=self.city,
                                                     size=self.grid_size)
    @property
    def query(self):
        DROP = ("""DROP TABLE IF EXISTS grids.{city}_urban_clusters_{size};"""
                .format(city=self.city, size=self.grid_size))
        QUERY =  ("""CREATE TABLE grids.{city}_urban_clusters_{size}
                     (cluster_id INT,
                      year_model INT,
                      population numeric,
                      built_threshold numeric,
                      population_threshold numeric,
                      geom geometry);"""
                    .format(city=self.city,
                            size=self.grid_size))
        return DROP + QUERY


class GenerateGridTables(luigi.WrapperTask):
    """
    Task that calls task for generating all grids features and
    clusters table that appear on `grid_tables_path`
    """
    city = luigi.Parameter()
    grid_size = luigi.Parameter()
    grid_tables_path = luigi.Parameter()

    def requires(self):
        tables_tasks = [GenerateUrbanClusterTable(self.city, self.grid_size)]
        grid_tables = utils.read_yaml(self.grid_tables_path)
        for table, cols in grid_tables.items():
            tables_tasks.append(GenerateTable(self.city,
                                              self.grid_size,
                                              table,
                                              cols))
        return tables_tasks


if __name__ == "__main__":
    yaml_file = 'grid_tables.yaml'
    city = 'amman'
    grid_size = 250
    esri = 32236
    tables = utils.read_yaml(yaml_file)
    # connection
    engine = utils.get_engine()
    generate_grid(city, esri, grid_size, engine)
    generate_urban_center_table(engine, city, grid_size)
    for table, cols in tables.items():
        q = generate_table(engine, city, grid_size, table, cols)
