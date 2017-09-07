import pdb
import subprocess
import argparse
import os
from dotenv import load_dotenv, find_dotenv

import utils

# Variables de ambiente
load_dotenv(find_dotenv())

class DownloadModels():
    def __init__(self, city,
                       grid_size,
                       path,
                       host,
                       user,
                       password,
                       database):

        self.city = city
        self.grid_size = grid_size
        self.path = path
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.engine = utils.get_engine()

    def _temp_best(self, model_type):
        query = ("""CREATE TABLE {city}_tmp  AS (
                WITH best AS (
                    SELECT model_id, cutoff
                    FROM results.evaluations
                    JOIN results.models m
                    USING (model_id)
                    WHERE metric = 'f1@' AND m.city = '{city}'
                    AND model_type = '{model_type}'
                    ORDER BY value DESC  LIMIT 1),
                best_join AS (
                    SELECT model_id, cell_id,
                    CASE WHEN score >= cutoff::float
                        THEN 1 ELSE NULL END AS prediction,
                    cell
                    FROM results.predictions
                    JOIN grids.{city}_grid_{grid_size}
                    USING(cell_id)
                    JOIN best USING (model_id))
                SELECT model_id, st_union(cell)
                FROM best_join
                WHERE prediction NOTNULL
                GROUP BY model_id);""".format(city=self.city,
                                                     grid_size=self.grid_size,
                                                     model_type=model_type))
        db_conn = self.engine.raw_connection()
        db_conn.cursor().execute(query)
        db_conn.commit()
        db_conn.close()
        print("temp table completes")

    def _delete_temp(self):
        drop = ("""DROP TABLE IF EXISTS {city}_tmp""".format(city=self.city))
        db_conn = self.engine.raw_connection()
        db_conn.cursor().execute(drop)
        db_conn.commit()
        db_conn.close()
        print("temp table deleted")

    def download_result(self, model_type):
        if not os.path.exists(self.path + '/' +  self.city):
            os.makedirs(self.path + '/' +  self.city)

        self._temp_best(model_type)
        cmd = (""" pgsql2shp -f {path}/{city}/{city}_{model}_prediction -h {host} -u {user} -P {password} {database} "SELECT * 
                   FROM {city}_tmp" """.format(city=self.city,
                                             model=model_type,
                                             path=self.path,
                                             host=self.host,
                                             user=self.user,
                                             password=self.password,
                                             database=self.database))
        subprocess.call(cmd, shell=True)
        self._delete_temp()
        print("download_result_{model_type}_done".format(model_type=model_type))

    def download_urban_clusters(self, year):
        if not os.path.exists(self.path + '/' +  self.city):
            os.makedirs(self.path + '/' +  self.city)

        cmd = ("""pgsql2shp -f {path}/{city}/{city}_urban_{year} -h {host} -u {user} -P {password} {database} "SELECT * 
                FROM grids.{city}_urban_clusters_{grid_size}
                WHERE year_model= {year}
                AND built_threshold=40
                AND population_threshold=75
                AND population >= 5000;" """.format(path=self.path,
                                                  city=self.city,
                                                  year=year,
                                                  grid_size=self.grid_size,
                                                  host=self.host,
                                                  user=self.user,
                                                  password=self.password,
                                                  database=self.database))
        subprocess.call(cmd, shell=True)
        print("download_urban_clusters_{year}_done".format(year=year))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--city", type=str, help="pass the city")
    parser.add_argument("--grid_size", type=int, help="pass the grid size ")
    parser.add_argument("--path", type=str, help="pass your directory for storing files")
    parser.add_argument("--years", type=list, help="pass years of years models", default=[1990, 2000, 2014])
    parser.add_argument("--model_types", type=list, help="pass model types", default=['RandomForest', 'ExtraTrees', 'LogisticRegression'])
    args = parser.parse_args()
    # Environment variables
    database = os.environ.get("PGDATABASE")
    user = os.environ.get("POSTGRES_USER")
    password = os.environ.get("POSTGRES_PASSWORD")
    host = os.environ.get("PGHOST")

    # Initiate Class
    results = DownloadModels(args.city,
                            args. grid_size,
                            args.path,
                            host, user, password, database)
    # Download urban
    for year in args.years:
        results.download_urban_clusters(year)

    for model in args.model_types:
        results.download_result(model)
