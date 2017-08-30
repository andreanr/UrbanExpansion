import luigi
import os
import pdb
import subprocess
from luigi import configuration
from luigi.contrib import postgres
from dotenv import load_dotenv,find_dotenv
from commons import city_task
from etl import etl_downloads
from etl import etl_insertdb

load_dotenv(find_dotenv())


class InsertDBTasks(luigi.WrapperTask):
    city = configuration.get_config().get('general','city')
    insert_tasks = configuration.get_config().get('data','uploads')
    insert_tasks = [x.strip() for x in list(insert_tasks.split(','))] 
    local_path = configuration.get_config().get('general','local_path')
    insert_scripts = configuration.get_config().get('general', 'insert_scripts')

    def requires(self):
        tasks = []
        for task_name in self.insert_tasks:
            try:
                years = configuration.get_config().get(task_name, 'years')
                years = [x.strip() for x in list(years.split(','))]
            except:
                years = []
            if len(years) > 0:
                for year in years:
                    run_task = eval(task_name)
                    tasks.append(run_task(self.city,
                                          self.local_path,
                                          self.insert_scripts,
                                          year))
            else:
                run_task = eval(task_name)
                tasks.append(run_task(self.city,
                                      self.local_path,
                                      self.insert_scripts))
        yield tasks


class InsertDBTask(city_task.PostgresTask):

    city = luigi.Parameter()
    local_path = luigi.Parameter()
    insert_scripts = luigi.Parameter()

    def requires(self):
        return etl_downloads.DownloadTasks()


class built_lds(InsertDBTask):
    year = luigi.Parameter()
    
    @property
    def update_id(self):
        return "built_lds" + "__" + self.city + ':' + self.year

    @property
    def table(self):
        return """raw.{city}_built_lds_{year}""".format(city=self.city,
                                                        year=self.year)
    @property
    def query(self):
        command_list = ['sh', self.insert_scripts + "built_lds.sh",
                        self.city,
                        self.year,
                        self.local_path]
        cmd = " ".join(command_list)
        
        subprocess.call([cmd], shell=True)
        with open(self.local_path + '/built_lds/' + self.year + '/built_lds_' +
                  self.city + '.sql', 'r') as myfile:
            query_str = myfile.read()

        return query_str


class city_lights(InsertDBTask):
    
    year = luigi.Parameter()
    
    @property
    def update_id(self):
        return "city_lights" + "__" + self.city + ':' + self.year

    @property
    def table(self):
        return """raw.{city}_city_lights_{year}""".format(city=self.city,
                                                          year=self.year)
    @property
    def query(self):
        command_list = ['sh', self.insert_scripts + "city_lights.sh",
                        self.city,
                        self.year,
                        self.local_path]
        cmd = " ".join(command_list)
        subprocess.call([cmd], shell=True)
        with open(self.local_path + '/city_lights/' + self.year + '/city_lights_' +
                  self.city + '.sql', 'r') as myfile:
            query_str = myfile.read()

        return query_str


class population(InsertDBTask):
    
    year = luigi.Parameter()
    
    @property
    def update_id(self):
        return "population" + "__" + self.city + ':' + self.year

    @property
    def table(self):
        return """raw.{city}_population_{year}""".format(city=self.city,
                                                         year=self.year)
    @property
    def query(self):
        command_list = ['sh', self.insert_scripts + "population.sh",
                        self.city,
                        self.year,
                        self.local_path]
        cmd = " ".join(command_list)
        subprocess.call([cmd], shell=True)
        with open(self.local_path + '/population/' + self.year + '/population_' +
                  self.city + '.sql', 'r') as myfile:
            query_str = myfile.read()

        return query_str


class settlements(InsertDBTask):
    
    year = luigi.Parameter()
    
    @property
    def update_id(self):
        return "settlements" + "__" + self.city + ':' + self.year

    @property
    def table(self):
        return """raw.{city}_settlements_{year}""".format(city=self.city,
                                                         year=self.year)
    @property
    def query(self):
        command_list = ['sh', self.insert_scripts + "settlements.sh",
                        self.city,
                        self.year,
                        self.local_path]
        cmd = " ".join(command_list)
        subprocess.call([cmd], shell=True)
        with open(self.local_path + '/settlements/' + self.year + '/settlements_' +
                  self.city + '.sql', 'r') as myfile:
            query_str = myfile.read()

        return query_str


class dem(InsertDBTask):
    
    @property
    def update_id(self):
        return "dem" + "__" + self.city

    @property
    def table(self):
        return """raw.{city}_dem""".format(city=self.city)

    @property
    def query(self):
        command_list = ['sh', self.insert_scripts + "dem.sh",
                        self.city,
                        self.local_path]
        cmd = " ".join(command_list)
        subprocess.call([cmd], shell=True)
        with open(self.local_path + '/dem/' + self.city + '_dem.sql', 'r') as myfile:
            query_str = myfile.read()

        return query_str


class water_bodies(InsertDBTask):
    
    @property
    def update_id(self):
        return "water_bodies" + "__" + self.city

    @property
    def table(self):
        return """raw.{city}_water_bodies""".format(city=self.city)

    @property
    def query(self):
        command_list = ['python', self.insert_scripts + "water_bodies.py",
                        '--city',
                        self.city,
                        '--local_path',
                        self.local_path]
        cmd = " ".join(command_list)
        subprocess.call([cmd], shell=True)
        with open(self.local_path + '/water_bodies/' + 'water_bodies_' + self.city + '.sql', 'r') as myfile:
            query_str = myfile.read()

        return query_str


class highways(InsertDBTask):
    
    @property
    def update_id(self):
        return "highways" + "__" + self.city

    @property
    def table(self):
        return """raw.{city}_highways""".format(city=self.city)

    @property
    def query(self):
        command_list = ['python ', self.insert_scripts + "highways.py",
                        '--city ', self.city,
                        '--local_path ', self.local_path]
        cmd = " ".join(command_list)
        subprocess.call([cmd], shell=True)
        with open(self.local_path + '/highways/' + 'highways_' +
                  self.city + '.sql', 'r') as myfile:
            query_str = myfile.read()

        return query_str


class geopins(InsertDBTask):
    
    @property
    def update_id(self):
        return "geopins" + "__" + self.city

    @property
    def table(self):
        return """raw.{city}_geopins""".format(city=self.city)

    @property
    def query(self):
        command_list = ['python3 ', self.insert_scripts + "geopins.py",
                        '--city ', self.city,
                        '--local_path ', self.local_path]
        cmd = " ".join(command_list)
        subprocess.call([cmd], shell=True)
        with open(self.local_path + '/geopins/' + 'geopins_' +
                  self.city + '.sql', 'r') as myfile:
            query_str = myfile.read()

        return query_str
