import datetime
import logging
import luigi
import psycopg2
import re
import tempfile

from grids import *

engine = utils.get_engine()

class GenerateGrids(luigi.Task):


    grid_size = luigi.Parameter() #250
    city = luigi.Parameter() #'amman'
    esri = luigi.Parameter() #32236
    time = luigi.Parameter() #1990
    built_threshold = luigi.Parameter() #50
    population_threshold = luigi.Parameter() #75
    cluster_threshold = luigi.Parameter() #5000
    year_model = luigi.Parameter() #1990

    def requires():

    def run():

    def output():

class UrbanGrids(luigi.Task):

    def requires():

    def run():

    def output():


class InputGrids(luigi.Task):

    def requires():

    def run():

    def output():


class UrbanGrids(luigi.Task):

    def requires():

    def run():

    def output():


#1. se puede paralelizar generar todos los tipos de grids excepto: 
# urban_clusters, urban_distance, urban_neighbours(ese aun no corre se estaba tardando mucho),
#2.  generar urban_clusters - que depende de population y built_lds
#3. urban_distance, urban_neighbours que dependen del urban_cluster

#lo que se me ocurría era solo cambiar el nombre al que se llama urban_center como a (city_center) y así es más fácil separar par lo primero todo lo que dice 'urban' no los corres. 
#despues urban_cluster
#y ya despues los que digan urba

#geopins(grid_size, city, esri, engine)
# print('urban_center')
#urban_center(grid_size, city, esri, engine)
# print('slope')
#slope(grid_size, city, esri, engine)
# print('highways')
#highways(grid_size, city, esri, engine)
# print('built_lds')
#built_lds(grid_size, city, time, esri, engine)
# print('dem')
#dem(grid_size, city, esri, engine)
# print('popuation')
#population(grid_size, city, time, esri, engine)
#print('city lights')
#city_lights(grid_size, city, time, esri, engine)
# print('settlements')
#settlements(grid_size, city, time, esri, engine)
#print('urban cluster')
#urban_clusters(grid_size, city, built_threshold, population_threshold, year_model, engine)
# print('built_distance')
#built_distance(grid_size, city, built_threshold, time, year_model, esri, engine)
#print('urban distance')
#urban_distance(grid_size, city, built_threshold, population_threshold, cluster_threshold, year_model, engine)
#print('urban neighbours')
#urban_neighbours(grid_size, city, built_threshold, population_threshold, cluster_threshold, year_model, engine)

