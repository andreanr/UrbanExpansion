import luigi
import os
import pdb
import subprocess
from luigi import configuration
from luigi.contrib import postgres
from dotenv import load_dotenv,find_dotenv
from commons import city_task
from etl.etl_insertdb import InsertDBTasks

load_dotenv(find_dotenv())

class slope(city_task.PostgresTask):
    city = luigi.Parameter()
    esri = luigi.Parameter()

    def requires(self):
        return InsertDBTasks()

    @property
    def update_id(self):
        return 'slope_raw:{city}'.format(city=self.city)

    @property
    def table(self):
        return """raw.{city}_slope""".format(city=self.city)

    @property
    def query(self):
        drop = ("""DROP TABLE IF EXISTS raw.{city}_slope;""".format(city=self.city))
        create = ("""CREATE TABLE raw.{city}_slope AS (
                    select
                        st_slope(st_transform(rast, {esri}) ,1,'32BF','PERCENT') as rast_percent,
                        st_slope(st_transform(rast, {esri}),1) as rast_degrees
                    from raw.{city}_dem);""".format(city=self.city,
                                                    esri=self.esri))
        return drop + create


class city_center(city_task.PostgresTask):
    city = luigi.Parameter()

    def requires(self):
        return InsertDBTasks()

    @property
    def update_id(self):
        return 'city_center_raw:{city}'.format(city=self.city)

    @property
    def table(self):
        return """raw.{city}_city_center""".format(city=self.city)

    @property
    def query(self):
        # Parameters
        latitude = configuration.get_config().get(self.city,'latitude')
        longitude = configuration.get_config().get(self.city,'longitude')
        buffer_size = configuration.get_config().get(self.city,'buffer_size')
        # drop and create
        drop = """DROP TABLE IF EXISTS raw.{city}_city_center;""".format(city=self.city)
        create = ("""CREATE TABLE raw.{city}_city_center as (
                    SELECT ST_buffer(ST_SetSRID(ST_MakePoint({longitude}, {latitude}), 4326), {buffer_size}) as geom
                    );""".format(city=self.city,
                                 longitude=longitude,
                                 latitude=latitude,
                                 buffer_size=buffer_size))
        return drop + create


class PreprocessTask(luigi.WrapperTask):
    city = configuration.get_config().get('general','city')
    esri = configuration.get_config().get('general','esri')

    def requires(self):
        yield city_center(self.city)
        yield slope(self.city, self.esri)
