import luigi
import os
import pdb
import subprocess
from luigi import configuration
from luigi.contrib import postgres
from dotenv import load_dotenv,find_dotenv

load_dotenv(find_dotenv())

class InsertDBTasks(luigi.WrapperTask):
    city = configuration.get_config().get('general','city')
    insert_tasks = configuration.get_config().get('data','uploads')
    insert_tasks = [x.strip() for x in list(upload_tasks.split(','))] 
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


###################
#   DATA INGEST
##################


class LocalUploadTask(luigi.Task):
    year = luigi.Parameter()
    city = luigi.Parameter()
    data_task = luigi.Parameter()
    insert_scripts = luigi.Parameter()
    local_path = luigi.Parameter()

    def output(self):
        if self.year:
            param_time = self.year #+ '/'
        else:
            param_time = ''
        return luigi.LocalTarget(self.local_path + '/' + self.data_task + '/' +
                                 param_time + '/' + self.data_task + self.file_type)

###################
#    EACH DATA
###################


class built_lds(LocalDownloadTask):
    file_type = '.zip'

    def run(self):
        if not os.path.exists(self.local_path + '/' + self.data_task):
            os.makedirs(self.local_path + '/' + self.data_task)
        if not os.path.exists(self.local_path + '/' + self.data_task + '/' + self.year):
            os.makedirs(self.local_path + '/' + self.data_task + '/' + self.year)
        command_list = ['sh', self.download_scripts + "built_lds.sh",
                        self.city,
                        self.year,
                        self.local_path,
                        self.data_task + self.file_type]
        cmd = " ".join(command_list)
        return subprocess.call([cmd], shell=True)
