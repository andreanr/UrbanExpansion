import luigi
import os
import pdb
from luigi import configuration
from luigi.contrib import postgres
from dotenv import load_dotenv,find_dotenv

load_dotenv(find_dotenv())

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


