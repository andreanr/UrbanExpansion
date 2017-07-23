import utils
import luigi

import pdb


class DropTmpTable(postgres.PostgresQuery):
    city = luigi.Parameter()

    def requirements(self):
        return PreprocessTask()

    @property
    def query(self):
        return ("""DROP TABLE IF EXISTS public.grids_{city}_temp"""
            .format(city=self.city))


class CreateTmpTable(postgres.PostgresQuery):
    city = luigi.Parameter()

    def requirements(self):
        return [PreprocessTask(),
                DropTmpTable(self.city)]

    @property
    def query(self):
        return  ("""CREATE TABLE public.grids_{city}_temp
                         (cell_id serial not null primary key)"""
                    .format(city=self.city))


class AddGeomColumn(postgres.PostgresQuery):
    city = luigi.Parameter()

    def requirements(self):
        return [CreateTmpTable(self.city)]

    @property
    def query(self):
        return ("""SELECT addgeometrycolumn('public', 'grids_{city}_temp','cell', 0, 'POLYGON', 2)"""
                .format(city=self.city))


class DropFunction(postgres.PostgresQuery):
    city = luigi.Parameter()

    def requirements(self):
        return AddGeomColumn(self.city)

    @property
    def query(self):
        return ("""DROP FUNCTION genhexagons(float, float, float, float, float)""")

class GenHexagonsSQL(postgres.PostgresQuery):
    city = luigi.Parameter()
    esri = luigi.Parameter()

    def requirements(self):
        return DropFunction(self.city)

    @property
    def query(self):
        return ("""CREATE OR REPLACE FUNCTION genhexagons(width float,
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
                    $total$ LANGUAGE plpgsql;""".format(city=self.city,
                                                        esri=self.esri))


class GenerateTmpGrid(postgres.PostgresQuery):
    city = luigi.Parameter()
    esri = luigi.Parameter()
    grid_size = luigi.Paremeter()

    def requirements(self):
        return GenHexagonsSQL(self.city,
                              self.esri)

    @property
    def query(self):
        return ("""WITH buffer_prj AS (
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
                       FROM geom_bbox""".format(city=self.city,
                                                 esri=self.esri,
                                                 grid_size=self.grid_size))


class GenerateGrid(postgres.PostgresQuery):
    city = luigi.Parameter()
    esri = luigi.Parameter()
    grid_size = luigi.Parameter()

    def requirements(self):
        return GenerateTmpGrid(self.city, self.esri, self.grid_size)

    @property
    def query(self):
        return ("""CREATE TABLE grids.{city}_grid_{grid_size} AS (
                        WITH buffer_prj AS (
                            SELECT st_transform(geom, {esri}) AS geom
                            FROM raw.{city})
                        SELECT grid.*
                        FROM public.grids_{city}_temp AS grid
                        JOIN buffer_prj
                        ON st_intersects(cell, geom)
                        );""".format(city=self.city,
                                     esri=self.esri,
                                     grid_size=self.grid_size))


class AlterGeometry(postgres.PostgresQuery):
    city = luigi.Parameter()
    esri = luigi.Parameter()
    grid_size = luigi.Parameter()

    def requirements(self):
        return GenerateGrid(self.city, self.esri, self.grid_size)

    @property
    def query(self):
        return ("""ALTER TABLE grids.{city}_grid_{size}
                    ALTER COLUMN cell TYPE geometry(Polygon, {esri})
                    USING ST_SetSRID(cell,{esri})""".format(city=self.city,
                                                            size=self.grid_size,
                                                            esri=self.esri))


class AddPrimaryKey(postgres.PostgresQuery):
    city = luigi.Parameter()
    esri = luigi.Parameter()
    grid_size = luigi.Parameter()

    def requirements(self):
        return AlterGeometry(self.city,
                             self.esri,
                             self.grid_size)

    @property
    def query(self):
        return ("""ALTER TABLE grids.{city}_grid_{size} ADD PRIMARY KEY (cell_id)"""
                  .format(city=self.city,
                          size=self.grid_size))


class GenerateTable(postgres.PostgresQuery):
    city = luigi.Parameter()
    grid_size = luigi.Parameter()
    name = luigi.Parameter()
    columns_dict = luigi.Parameter()

    def requirements(self):
        return Preprocess()

    @property
    def query(self):
        columns_ref = []
        for col, ref in self.columns_dict.items():
            columns_ref.append(""" {col}   {ref} """.format(col=col,
                                                            ref=ref))
        columns_ref = ", ".join(columns_ref)
        QUERY = ("""CREATE TABLE grids.{city}_{name}_{size} (
                    cell_id  INT REFERENCES grids.{city}_grid_{size} (cell_id),
                    {columns_ref}
                )""".format(city=self.city,
                            size=self.grid_size,
                            name=self.name,
                            columns_ref=columns_ref))
        return QUERY


class GenerateUrbanClusterTable(postgres.PostgresQuery):
    city = luigi.Parameter()
    grid_size = luigi.Parameter()

    def requirements(self):
        return Preprocess()

    @property
    def query(self):
        return ("""CREATE TABLE grids.{city}_urban_clusters_{size}
                     (cluster_id INT,
                      year_model INT,
                      population numeric,
                      built_threshold numeric,
                      population_threshold numeric,
                      geom geometry)"""
                    .format(city=self.city,
                            size=self.grid_size))


class GenerateGridTables(luigi.Wrapper):
    city = luigi.Parameter()
    grid_size = luigi.Parameter()
    grid_tables_path = luigi.Parameter()

    def requirements(self):
        tables_tasks = [GenerateUrbanClusterTable(self.city, self.grid_size)]
        grid_tables = utils.utils.read_yaml(self.grid_tables_path)
        for table, cols in tables.items():
            tables_tasks.append(GenerateTable(self.city,
                                              self.grid_size,
                                              table,
                                              cols))
        return table_tasks


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
