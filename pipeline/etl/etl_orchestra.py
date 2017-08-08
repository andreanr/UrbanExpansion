import luigi
import os
import pdb
import subprocess
from luigi import configuration
from luigi.contrib import postgres
from dotenv import load_dotenv,find_dotenv

load_dotenv(find_dotenv())


class DownloadTasks(luigi.WrapperTask):
    city = configuration.get_config().get('general','city')
    download_tasks = configuration.get_config().get('data', 'downloads')
    download_tasks = [x.strip() for x in list(download_tasks.split(','))]
    local_path = configuration.get_config().get('general', 'local_path')
    download_scripts = configuration.get_config().get('general', 'download_scripts')
    
    def requires(self):
        tasks = []
        for task_name in self.download_tasks:
            try:
                years = configuration.get_config().get(task_name, 'years')
                years = [x.strip() for x in list(years.split(','))]
            except:
                years = []
            if len(years)>0:
                for year in years:
                    run_task = eval(task_name)
                    tasks.append(run_task(year, 
                                          self.city, 
                                          task_name,
                                          self.download_scripts,
                                          self.local_path))
            else:
                run_task = eval(task_name)
                tasks.append(run_task('',
                                      self.city,
                                      task_name,
                                      self.download_scripts,
                                      self.local_path))
        yield tasks


class CreateSchema(postgres.PostgresQuery):
    # RDS connection
    database = os.environ.get("PGDATABASE")
    user = os.environ.get("POSTGRES_USER")
    password = os.environ.get("POSTGRES_PASSWORD")
    host = os.environ.get("PGHOST")

    # schema to query parameter
    query = luigi.Parameter()
    table = ''

    def run(self):
        connection = self.output().connect()
        cursor = connection.cursor()
        sql = "Create schema {schema}".format(schema=self.query)

        print(sql)
        cursor.execute(sql)

        # Update marker table
        self.output().touch(connection)

        # commit and close connection
        connection.commit()
        connection.close()


class CreateSchemas(luigi.WrapperTask):
    schemas = configuration.get_config().get('schemas', 'names')

    def requires(self):
        yield [CreateSchema(query) for
                query in self.schemas.split(',')]

###################
#   DATA INGEST
##################


class LocalDownloadTask(luigi.Task):
    year = luigi.Parameter()
    city = luigi.Parameter()
    data_task = luigi.Parameter()
    download_scripts = luigi.Parameter()
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


class population(LocalDownloadTask):
    file_type = '.zip'

    def run(self):
        if not os.path.exists(self.local_path + '/' + self.data_task):
            os.makedirs(self.local_path + '/' + self.data_task)
        if not os.path.exists(self.local_path + '/' + self.data_task + '/' + self.year):
            os.makedirs(self.local_path + '/' + self.data_task + '/' + self.year)

        command_list = ['sh', self.download_scripts + "population.sh",
                        self.city,
                        self.year,
                        self.local_path,
                        self.data_task + self.file_type]
        cmd = " ".join(command_list)
        return subprocess.call([cmd], shell=True)


class settlements(LocalDownloadTask):
    file_type = '.zip'

    def run(self):
        if not os.path.exists(self.local_path + '/' + self.data_task):
            os.makedirs(self.local_path + '/' + self.data_task)
        if not os.path.exists(self.local_path + '/' + self.data_task + '/' + self.year):
            os.makedirs(self.local_path + '/' + self.data_task + '/' + self.year)

        command_list = ['sh', self.download_scripts + "settlements.sh",
                        self.city,
                        self.year,
                        self.local_path,
                        self.data_task + self.file_type]
        cmd = " ".join(command_list)
        return subprocess.call([cmd], shell=True)


class city_lights(LocalDownloadTask):
    file_type = '.tgz'

    def run(self):
        if not os.path.exists(self.local_path + '/' +self.data_task):
            os.makedirs(self.local_path + '/' + self.data_task)
        if not os.path.exists(self.local_path + '/' + self.data_task + '/' + self.year):
            os.makedirs(self.local_path + '/' + self.data_task + '/' + self.year)

        command_list = ['sh', self.download_scripts + "city_lights.sh",
                        self.city,
                        self.year,
                        self.local_path,
                        self.data_task + self.file_type]
        cmd = " ".join(command_list)
        return subprocess.call([cmd], shell=True)


class water_bodies(LocalDownloadTask):
    file_type = '.zip'

    def run(self):
        if not os.path.exists(self.local_path + '/' + self.data_task):
            os.makedirs(self.local_path + '/' + self.data_task)
        command_list = ['sh', self.download_scripts + "water_bodies.sh",
                        self.city,
                        self.local_path,
                        self.data_task + self.file_type]
        cmd = " ".join(command_list)
        return subprocess.call([cmd], shell=True)


class dem(LocalDownloadTask):
    file_type = '.zip'

    def run(self):
        if not os.path.exists(self.local_path + '/' + self.data_task):
            os.makedirs(self.local_path + '/' + self.data_task)
        command_list = ['sh', self.download_scripts + "dem.sh",
                        self.city,
                        self.local_path,
                        self.data_task + self.file_type]
        cmd = " ".join(command_list)
        return subprocess.call([cmd], shell=True)


class shp_buffer(LocalDownloadTask):
    file_type = '.json'
    buffer_dist = luigi.Parameter()

    def run(self):
        if not os.path.exists(self.local_path + '/' + self.data_task):
            os.makedirs(self.local_path + '/' + self.data_task)

        command_list = ['python', self.download_scripts + "shp_buffer.py",
                        '--city', self.city,
                        '--buffer', self.buffer_dist,
                        '--local_path', self.local_path,
                        '--data_task', self.data_task]
        cmd = " ".join(command_list)
        print(cmd)
        return subprocess.call([cmd], shell=True)



