import luigi
import os
import pdb
import subprocess
from luigi import configuration
from luigi.contrib import postgres
from dotenv import load_dotenv,find_dotenv
from commons import city_task

load_dotenv(find_dotenv())

#class InsertDBTasks(luigi.WrapperTask):
#    city = configuration.get_config().get('general','city')
#    insert_tasks = configuration.get_config().get('data','uploads')
#    insert_tasks = [x.strip() for x in list(upload_tasks.split(','))] 
#    local_path = configuration.get_config().get('general','local_path')
#    insert_scripts = configuration.get_config().get('general', 'insert_scripts')
#
#    def requires(self):
#        tasks = []
#        for task_name in self.insert_tasks:
#            try:
#                years = configuration.get_config().get(task_name, 'years')
#                years = [x.strip() for x in list(years.split(','))]
#            except:
#                years = []
#            if len(years) > 0:
#                for year in years:
#                    tasks.append(


class built_lds(city_task.PostgresTask):
    
    city = 'irbid' # luigi.Parameter()
    year = '2000' #luigi.Parameter()
    local_path = '/home/data' # luigi.Parameter()
    insert_scripts = 'etl/insert_db/' # luigi.Parameter()
    
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


class city_lights(city_task.PostgresTask):
    
    city = 'irbid' # luigi.Parameter()
    year = '2000' #luigi.Parameter()
    local_path = '/home/data' # luigi.Parameter()
    insert_scripts = 'etl/insert_db/' # luigi.Parameter()
    
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


class population(city_task.PostgresTask):
    
    city = 'irbid' # luigi.Parameter()
    year = '1990' #luigi.Parameter()
    local_path = '/home/data' # luigi.Parameter()
    insert_scripts = 'etl/insert_db/' # luigi.Parameter()
    
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


class settlements(city_task.PostgresTask):
    
    city = 'irbid' # luigi.Parameter()
    year = '1990' #luigi.Parameter()
    local_path = '/home/data' # luigi.Parameter()
    insert_scripts = 'etl/insert_db/' # luigi.Parameter()
    
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


class dem(city_task.PostgresTask):
    
    city = 'irbid' # luigi.Parameter()
    local_path = '/home/data' # luigi.Parameter()
    insert_scripts = 'etl/insert_db/' # luigi.Parameter()
    
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


class water_bodies(city_task.PostgresTask):
    
    city = 'irbid' # luigi.Parameter()
    local_path = '/home/data' # luigi.Parameter()
    insert_scripts = 'etl/insert_db/' # luigi.Parameter()
    
    @property
    def update_id(self):
        return "water_bodies" + "__" + self.city

    @property
    def table(self):
        return """raw.{city}_water_bodies""".format(city=self.city)

    @property
    def query(self):
        command_list = ['sh', self.insert_scripts + "water_bodies.sh",
                        self.city,
                        self.local_path]
        cmd = " ".join(command_list)
        subprocess.call([cmd], shell=True)
        with open(self.local_path + '/water_bodies/' + 'water_bodies_' +
                  self.city + '.sql', 'r') as myfile:
            query_str = myfile.read()

        return query_str
