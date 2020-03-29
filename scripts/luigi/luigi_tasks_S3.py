#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Librerias necesarias para la ejecución de las tareas
import boto3
import json
import luigi
import luigi.contrib.s3
import os
import pandas as pd

from sodapy import Socrata
from pyspark.sql import SQLContext
from pyspark import SparkContext
from dynaconf import settings

# Definir los paths donde se guardan los datos
path_raw = 's3://raw'
path_preproc = 's3://preprocess'


# Path para guardar los datos
class downloadRawJSONData(luigi.Task):
    '''
    Tarea: Descarga los datos de la API de 311 NYC en formato JSON en carpetas por
    fecha con frecuencia diaria.

    Input: Información de la API 311 NYC

    Output: Documentos en formato .json ubicados en las carpetas en S3

    Dependencias: Ninguna
    '''
    # Parámetros requeridos al usuario:
    year = luigi.Parameter()
    month = luigi.Parameter()
    day = luigi.Parameter()
    bucket_name = luigi.Parameter(default="prueba-nyc311")

    # Defining the path where the data will be saved.
    def output(self):
        # If the specified path isn't avaible, it will generate the path.
        output_path = f"{path_raw}/{self.year}/{self.month}/{self.day}/data_{self.year}_{self.month}_{self.day}.json"
        return luigi.contrib.s3.S3Target(path=output_path)

    # Defining the bussiness logic of the task
    def run(self):
        # Autenticación en S3
        ses = boto3.session.Session(
            profile_name={self.bucket_name}, region_name='us-west-2')
        s3_resource = ses.resource('s3')

        # Autenticación del cliente, ligado al documento "settings.toml":
        client = Socrata(settings.get('dburl'),
                         settings.get('apptoken'),
                         username=settings.get('user'),
                         password=settings.get('pass'))

        # Los resultados son retornados como un archivo JSON desde la API /
        # convertida a una lista de Python usando sodapy
        client.timeout = 1000
        limit = 1000000000

        # Loop para generar carpetas/path donde serán guardados los datos:
        # Crear carpeta raw
        if not os.path.exists(f'{path_raw}'):
            os.mkdir(f'{path_raw}')
        else:
            None

        # Crear carpeta year
        if not os.path.exists(f'{path_raw}/{self.year}'):
            os.mkdir(f'{path_raw}/{self.year}')
        else:
            None

        # Crear carpeta year/month
        if not os.path.exists(f'{path_raw}/{self.year}/{self.month}'):
            os.mkdir(f'{path_raw}/{self.year}/{self.month}')
        else:
            None

        # Crear carpeta  year/month/day
        if not os.path.exists(f'{path_raw}/{self.year}/{self.month}/{self.day}'):
            os.mkdir(f'{path_raw}/{self.year}/{self.month}/{self.day}')
        else:
            None

        # Query to dump data retrieved in a .json file
        results = client.get(
            "erm2-nwe9", limit=limit, where=f"created_date between '{self.year}-{self.month}-{self.day}T00:00:00.000' and '{self.year}-{self.month}-{self.day}T23:59:59.999'")
        with self.output().open('w') as json_file:
            json.dump(results, json_file)


class preprocParquetPandas(luigi.Task):
    '''
    Tarea: Convierte datos descargados en .JSON a formato .PARQUET., empleando
    la librería pandas.

    Input: documentos .json , previamente descargados en el task /
    "downloadRawJSONData"

    Output: documentos en formato .parquet ubicados en carpetas en S3

    Dependencias: downloadRawJSONData
    '''
    # Parámetros requeridos al usuario
    year = luigi.Parameter()
    month = luigi.Parameter()
    day = luigi.Parameter()

    # Definiendo dependecias con tasks previos
    def requires(self):
        return downloadRawJSONData(year=self.year, month=self.month, day=self.day)

    # Defining where it writes the output
    def output(self):
        output_path = f"{path_preproc}/{self.year}/{self.month}/{self.day}/data_{self.year}_{self.month}_{self.day}.parquet"
        return luigi.contrib.s3.S3Target(path=output_path)

    # Defining the bussiness logic of the task
    def run(self):
        # Autenticación en S3
        ses = boto3.session.Session(
            profile_name={self.bucket_name}, region_name='us-west-2')
        s3_resource = ses.resource('s3')

        # Loop para generar carpetas/path preprocess, donde serán guardados los datos .parquet:

        # Crear carpeta preprocess
        if not os.path.exists(f'{path_preproc}'):
            os.mkdir(f'{path_preproc}')
        else:
            None

        # Crear carpeta year
        if not os.path.exists(f'{path_preproc}/{self.year}'):
            os.mkdir(f'{path_preproc}/{self.year}')
        else:
            None

        # Crear carpeta year/month
        if not os.path.exists(f'{path_preproc}/{self.year}/{self.month}'):
            os.mkdir(f'{path_preproc}/{self.year}/{self.month}')
        else:
            None

        # Crear carpeta  year/month/day
        if not os.path.exists(f'{path_preproc}/{self.year}/{self.month}/{self.day}'):
            os.mkdir(f'{path_preproc}/{self.year}/{self.month}/{self.day}')
        else:
            None

        # Convertir a .parquet usando la librería pandas

        # Línea para correrlo sin requirements de downloadRawJSONData
        # df = pd.read_json(f"{path_raw}/{self.year}/{self.month}/{self.day}/data_{self.year}_{self.month}_{self.day}.json")
        # Líneas para correrlo con requirements de downloadRawJSONData

        df = pd.read_json(self.input().path)
        # Solving problems of datatype: "nested column branch had multiple children"
        for col in df.columns:
            weird = (df[[col]].applymap(type) !=
                     df[[col]].iloc[0].apply(type)).any(axis=1)
            if len(df[weird]) > 0:
                df[col] = df[col].astype(str)
            if df[col].dtype == list:
                df[col] = df[col].astype(str)

        # Guardar como parquet
        self.output().makedirs()
        df.to_parquet(self.output().path, engine='auto', compression='snappy')


class preprocParquetSpark(luigi.Task):
    '''
    Tarea: Convierte datos descargados en .JSON a formato .PARQUET., empleando
    la librería pyspark.

    Input: documentos .json , previamente descargados en el task /
    "downloadRawJSONData"

    Output: documentos en formato .parquet ubicados en carpetas en S3

    Dependencias: downloadRawJSONData
    '''
    # Parámetros
    year = luigi.Parameter()
    month = luigi.Parameter()
    day = luigi.Parameter()

    # Definiendo dependencias con tasks previos
    def requires(self):
        return downloadRawJSONData(year=self.year, month=self.month, day=self.day)

    # Defining where it writes the output
    def output(self):
        output_path = f"{path_preproc}/{self.year}/{self.month}/{self.day}/"
        return luigi.contrib.s3.S3Target(path=output_path)

    # Defining the bussiness logic of the task
    def run(self):
        # Autenticación en S3
        ses = boto3.session.Session(
            profile_name={self.bucket_name}, region_name='us-west-2')
        s3_resource = ses.resource('s3')
        # Loop para generar carpetas/path preprocess, donde serán guardados los datos .parquet:

        # Crear carpeta preprocess
        if not os.path.exists(f'{path_preproc}'):
            os.mkdir(f'{path_preproc}')
        else:
            None

        # Crear carpeta year
        if not os.path.exists(f'{path_preproc}/{self.year}'):
            os.mkdir(f'{path_preproc}/{self.year}')
        else:
            None

        # Crear carpeta year/month
        if not os.path.exists(f'{path_preproc}/{self.year}/{self.month}'):
            os.mkdir(f'{path_preproc}/{self.year}/{self.month}')
        else:
            None

        # Crear carpeta  year/month/day
        if not os.path.exists(f'{path_preproc}/{self.year}/{self.month}/{self.day}'):
            os.mkdir(f'{path_preproc}/{self.year}/{self.month}/{self.day}')
        else:
            None

        # Convertir a parquet usando pyspark:
        # Crear sesión en spark
        sc = SparkContext.getOrCreate()
        sqlContext = SQLContext(sc)

        # Lineas para correrlo sin y con requirements de downloadRawJSONData
        # df = sqlContext.read.json(f"{path_raw}/{self.year}/{self.month}/{self.day}/data_{self.year}_{self.month}_{self.day}.json")

        # Linea para correrlo con requirements de downloadRawJSONData
        df = sqlContext.read.json(self.input().path)

        # Guardar como parquet
        self.output().makedirs()
        df.write.parquet(self.output().path, mode="overwrite")
