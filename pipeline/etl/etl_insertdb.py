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
    
    city = 'amman' # luigi.Parameter()
    year = '1990' #luigi.Parameter()
    local_path = '/home/data' # luigi.Parameter()
    insert_scripts = 'etl/insert_db/' # luigi.Parameter()
    
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
        pdb.set_trace()
        subprocess.call([cmd], shell=True)
        pdb.set_trace()
        with open(self.local_path + '/built_lds/' + self.year + '/built_lds_' +
                  self.city + '.sql', 'r') as myfile:
            query = myfile.read()
        pdb.set_trace()
        return query


