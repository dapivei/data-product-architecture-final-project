#!/usr/bin/env python
# -*- coding: utf-8 -*-

import json
import boto3
import json
import luigi
import luigi.contrib.s3
import os
import pandas as pd
import functions  # modulo propio

from datetime import date
from dynaconf import settings
from pyspark.sql import SQLContext
from pyspark import SparkContext
from sodapy import Socrata

# Definir los paths donde se guardan los datos
path_raw = 's3://prueba-nyc311/raw'
path_preproc = 's3://prueba-nyc311/preprocess'

class downloadRawJSONData(luigi.Task):
    '''
    Descarga los datos de la API de 311 NYC en formato JSON en carpetas por
    fecha con frecuencia diaria.
    '''
    # parametros:
    bucket = luigi.Parameter(default="prueba-nyc311")
    year = luigi.Parameter()
    month = luigi.Parameter()
    day = luigi.Parameter()

    def output(self):
        output_path = f"{path_raw}/{self.year}/{self.month}/{self.day}/data_{self.year}_{self.month}_{self.day}.json"
        return luigi.contrib.s3.S3Target(path=output_path)

    def run(self):
        # Autenticación en S3
        ses = boto3.session.Session(
            profile_name='luigi_dpa', region_name='us-west-2')
        s3_resource = ses.resource('s3')

        obj = s3_resource.Bucket(self.bucket)
        print(ses)
        # Autenticación del cliente:
        client = Socrata(settings.get('dburl'),
                         settings.get('apptoken'),
                         username=settings.get('user'),
                         password=settings.get('pass'))

        # los resultados son retornados como un archivo JSON desde la API /
        # convertida a una lista de Python usando sodapy
        client.timeout = 1000
        limit = 1000000000

        # query
        results = client.get(
            "erm2-nwe9", limit=limit, where=f"created_date between '{self.year}-{self.month}-{self.day}T00:00:00.000' and '{self.year}-{self.month}-{self.day}T23:59:59.999'")
        with self.output().open('w') as json_file:
            json.dump(results, json_file)


class preprocParquetPandas(luigi.Task):
    '''
    Convertir datos descargados en JSON a formato PARQUET.
    '''
    # parametros
    bucket = luigi.Parameter(default="prueba-nyc311")
    year = luigi.Parameter()
    month = luigi.Parameter()
    day = luigi.Parameter()

    def requires(self):
        return downloadRawJSONData(bucket=self.bucket, year=self.year, month=self.month, day=self.day)

    def output(self):
        output_path = f"{path_preproc}/{self.year}/{self.month}/{self.day}/data_{self.year}_{self.month}_{self.day}.parquet"
        return luigi.contrib.s3.S3Target(path=output_path)

    def run(self):
        # Autenticación en S3
        ses = boto3.session.Session(profile_name='luigi_dpa', region_name='us-west-2')
        s3_resource = ses.resource('s3')

        obj = s3_resource.Bucket(self.bucket)
        print(ses)

        # crear carpeta preprocess
        if not os.path.exists(f'{path_preproc}'):
            os.mkdir(f'{path_preproc}')
        else:
            None

        # crear carpeta year
        if not os.path.exists(f'{path_preproc}/{self.year}'):
            os.mkdir(f'{path_preproc}/{self.year}')
        else:
            None

        # crear carpeta year/month
        if not os.path.exists(f'{path_preproc}/{self.year}/{self.month}'):
            os.mkdir(f'{path_preproc}/{self.year}/{self.month}')
        else:
            None

        # crear carpeta  year/month/day
        if not os.path.exists(f'{path_preproc}/{self.year}/{self.month}/{self.day}'):
            os.mkdir(f'{path_preproc}/{self.year}/{self.month}/{self.day}')
        else:
            None

        # convertir a parquet usando pandas
        df = pd.read_json(self.input().path)
        # Solving problems of datatype: "nested column branch had multiple children"
        for col in df.columns:
            weird = (df[[col]].applymap(type) !=
                     df[[col]].iloc[0].apply(type)).any(axis=1)
            if len(df[weird]) > 0:
                df[col] = df[col].astype(str)
            if df[col].dtype == list:
                df[col] = df[col].astype(str)

        # guardar como parquet
        self.output().makedirs()
        df.to_parquet(self.output().path, engine='auto', compression='snappy')


class preprocParquetSpark(luigi.Task):
    '''
    Convertir datos descargados en JSON a formato PARQUET.
    '''
    # parametros
    bucket = luigi.Parameter(default="prueba-nyc311")
    year = luigi.Parameter()
    month = luigi.Parameter()
    day = luigi.Parameter()

    def requires(self):
        return downloadRawJSONData(bucket=self.bucket,year=self.year, month=self.month, day=self.day)

    def output(self):
        output_path = f"{path_preproc}/{self.year}/{self.month}/{self.day}/"
        return luigi.contrib.s3.S3Target(path=output_path)

    def run(self):
        # Autenticación en S3
        ses = boto3.session.Session(profile_name='luigi_dpa', region_name='us-west-2')
        s3_resource = ses.resource('s3')

        obj = s3_resource.Bucket(self.bucket)
        print(ses)

        # crear carpeta preprocess
        if not os.path.exists(f'{path_preproc}'):
            os.mkdir(f'{path_preproc}')
        else:
            None

        # crear carpeta year
        if not os.path.exists(f'{path_preproc}/{self.year}'):
            os.mkdir(f'{path_preproc}/{self.year}')
        else:
            None

        # crear carpeta year/month
        if not os.path.exists(f'{path_preproc}/{self.year}/{self.month}'):
            os.mkdir(f'{path_preproc}/{self.year}/{self.month}')
        else:
            None

        # crear carpeta  year/month/day
        if not os.path.exists(f'{path_preproc}/{self.year}/{self.month}/{self.day}'):
            os.mkdir(f'{path_preproc}/{self.year}/{self.month}/{self.day}')
        else:
            None

        # convertir a parquet usando pyspark:
        # crear sesión en spark
        sc = SparkContext.getOrCreate()
        sqlContext = SQLContext(sc)
        # lineas para correrlo sin y con requirements de downloadRawJSONData
        # df = sqlContext.read.json(f"{path_raw}/{self.year}/{self.month}/{self.day}/data_{self.year}_{self.month}_{self.day}.json")
        df = sqlContext.read.json(self.input().path)

        # guardar como parquet
        self.output().makedirs()
        df.write.parquet(self.output().path, mode="overwrite")
