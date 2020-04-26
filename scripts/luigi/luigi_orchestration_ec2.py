#!/usr/bin/env python
# -*- coding: utf-8 -*-

import json
import boto3
import botocore
import datetime
import getpass
import luigi
import luigi.contrib.s3
import os
import pandas as pd
import platform
import psycopg2 as ps
import pyarrow as pa
import s3fs
import socket

from functionsV2 import queryApi311
from functionsV2 import execv

from datetime import date
from dynaconf import settings
from luigi.contrib.s3 import S3Client, S3Target
from pyspark import SparkContext
from pyspark.sql import SQLContext
from sodapy import Socrata

# ===================== Clases para guardar metadatos  =========================
# Se definen dos clases que guarden las características de los metadatos
# Nota: A este momento las dos clases son idénticas. Se van a realizar ajustes
# para que tengan características específicas:
# raw_metadata
# - corregir status de tarea de luigi
# - Size: tamaño de object en bucket de S3.
# preproc_metadata
# - Archivo de origen: (key de S3 object)
# - Size: tamaño de object en bucket de S3.
# - corregir status de tarea de luigi
# ==============================================================================

# ========== schema raw  ==========


class raw_metadata():
    def __init__(self,
                 name="",
                 extention="json",
                 schema="raw",
                 action="download from NYC 311 API",
                 creator="-",
                 machine="",
                 localhost="",
                 ip="",
                 creation_date="",
                 size="-",
                 location="",
                 status="sucess",
                 param_year="",
                 param_month="",
                 param_day="",
                 param_bucket=""):

        # asignamos las características de los metadatos
        self.name = name
        self.extention = extention
        self.schema = schema
        self.action = action
        self.creator = creator
        self.machine = machine
        self.ip = ip
        self.creation_date = creation_date
        self.size = size
        self.location = location
        self.status = status
        self.param_year = param_year
        self.param_month = param_month
        self.param_day = param_day
        self.param_bucket = param_bucket

    def info(self):
        return (self.name, self.extention, self.schema, self.action,
                self.creator, self.machine, self.ip, self.creation_date,
                self.size, self.location, self.status, self.param_year,
                self.param_month, self.param_day, self.param_bucket)

# ========== schema preprocess  ==========


class preproc_metadata():
    def __init__(self,
                 name="",
                 extention="parquet",
                 schema="preprocess",
                 action="transform JSON to parquet",
                 creator="-",
                 machine="",
                 localhost="",
                 ip="",
                 creation_date="",
                 size="-",
                 location="",
                 status="sucess",
                 param_year="",
                 param_month="",
                 param_day="",
                 param_bucket=""):

        # asignamos las características de los metadatos
        self.name = name
        self.extention = extention
        self.schema = schema
        self.action = action
        self.creator = creator
        self.machine = machine
        self.ip = ip
        self.creation_date = creation_date
        self.size = size
        self.location = location
        self.status = status
        self.param_year = param_year
        self.param_month = param_month
        self.param_day = param_day
        self.param_bucket = param_bucket

    def info(self):
        return (self.name, self.extention, self.schema, self.action,
                self.creator, self.machine, self.ip, self.creation_date,
                self.size, self.location, self.status, self.param_year,
                self.param_month, self.param_day, self.param_bucket)

###############################################################################

path_raw = 's3://prueba-nyc311/raw'
path_preproc = 's3://prueba-nyc311/preprocess'
path_cleaned = 's3://prueba-nyc311/cleaned'

class Task_10_download(luigi.Task):
    '''
    Task 1.0: Ingesta de datos de la API de 311 NYC en formato JSON en carpetas por fecha con frecuencia diaria.
    / esta versión importa del modulo functionsV2 la petición hecha a la API.
    '''
    # ==============================
    # parametros:
    # ==============================
    bucket = luigi.Parameter(default="prueba-nyc311")
    year = luigi.Parameter()
    month = luigi.Parameter()
    day = luigi.Parameter()
    # ==============================

    def output(self):
        # guarda los datos en s3://prueba-nyc311/raw/...
        output_path = f"{path_raw}/{self.year}/{self.month}/{self.day}/data_{self.year}_{self.month}_{self.day}.json"
        return luigi.contrib.s3.S3Target(path=output_path)

    def run(self):
        # Autenticación en S3
        ses = boto3.session.Session(
            profile_name='luigi_dpa', region_name='us-west-2')
        s3_resource = ses.resource('s3')
        obj = s3_resource.Bucket(self.bucket)

        # query a la API
        results = queryApi311(self.year, self.month, self.day)

        with self.output().open('w') as json_file:
            json.dump(results, json_file)


class Task_20_metaDownload(luigi.task.WrapperTask):
    '''
    Guardar los metadatos de la descarga de datos del schema RAW
    Son guardados en la base de datos nyc311_metadata en la tabla raw.etl_execution
    '''
    # ==============================
    # parametros:
    # ==============================
    bucket = luigi.Parameter(default="prueba-nyc311")
    year = luigi.Parameter()
    month = luigi.Parameter()
    day = luigi.Parameter()
    # ==============================

    def requires(self):
        return Task_10_download(year=self.year, month=self.month, day=self.day)

    def run(self):
        # se instancia la clase raw_metadata()
        cwd = os.getcwd()  # directorio actual
        raw_meta = preproc_metadata()
        raw_meta.name = f"data_{self.year}_{self.month}_{self.day}"
        raw_meta.user = str(getpass.getuser())
        raw_meta.machine = str(platform.platform())
        raw_meta.ip = execv("curl ipecho.net/plain ; echo", cwd)
        raw_meta.creation_date = str(datetime.datetime.now())
        raw_meta.location = f"{path_raw}/{raw_meta.name}"
        raw_meta.param_year = str(self.year)
        raw_meta.param_month = str(self.month)
        raw_meta.param_day = str(self.day)
        raw_meta.param_bucket = str(self.bucket)

        ubicacion_completa = f"{raw_meta.location}.json"
        meta = raw_meta.info()  # extrae info de la clase

        print("=" * 100)
        print(meta)
        print("complete name: ", ubicacion_completa)
        print("name: ", raw_meta.name)
        print("extensión: ", raw_meta.extention)
        print("schema: ", raw_meta.schema)
        print("tamaño: ", raw_meta.size)
        print("action: ", raw_meta.action)
        print("usuario: ", raw_meta.user)
        print("maquina: ", raw_meta.machine)
        print("ip: ", raw_meta.ip)
        print("fecha de creación: ", raw_meta.creation_date)
        print("ubicación: ", raw_meta.location)
        print("param [year]: ", raw_meta.param_year)
        print("param [month]: ", raw_meta.param_month)
        print("param [day]: ", raw_meta.param_day)
        print("param [bucket]: ", raw_meta.param_bucket)
        print("=" * 100)

        # conectarse a la base de datos y guardar a esquema raw.etl_execution
        conn = ps.connect(host=settings.get('host'),
                          port=settings.get('port'),
                          database="nyc311_metadata",
                          user=settings.get('usr'),
                          password=settings.get('password'))
        cur = conn.cursor()
        columns = "(name, extention, schema, action, creator, machine, ip, creation_date, size, location,status, param_year, param_month, param_day, param_bucket)"
        sql = "INSERT INTO raw.etl_execution" + columns + \
            " VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);"
        cur.execute(sql, meta)
        conn.commit()
        cur.close()
        conn.close()


class Task_30_preproc(luigi.Task):
    '''
    Convertir datos descargados en JSON y los transforma a formato parquet utilizando pandas.
    '''
    # ==============================
    # parametros:
    # ==============================
    bucket = luigi.Parameter(default="prueba-nyc311")
    year = luigi.Parameter()
    month = luigi.Parameter()
    day = luigi.Parameter()
    # ==============================
    def requires(self):
        return Task_20_metaDownload(year=self.year, month=self.month, day=self.day)

    def output(self):
        # guarda los datos en s3://prueba-nyc311/raw/.3..
        output_path = f"s3://{self.bucket}/preprocess/{self.year}/{self.month}/{self.day}/data_{self.year}_{self.month}_{self.day}.parquet"
        return luigi.contrib.s3.S3Target(path=output_path)

    def run(self):
        # Autenticación en S3
        ses = boto3.session.Session(
            profile_name='luigi_dpa', region_name='us-west-2')
        s3_resource = ses.resource('s3')
        obj = s3_resource.Bucket(name=self.bucket)
        # key (ruta) del archivo JSON en el bucket
        key = f"raw/{self.year}/{self.month}/{self.day}/data_{self.year}_{self.month}_{self.day}.json"
        json_object = s3_resource.Object(bucket_name=self.bucket, key=key) # objeto
        data_json_object = json_object.get()['Body'].read().decode('utf-8') # info del objeto
        df = pd.read_json(data_json_object) # lectura en pandas
        # Solving  problems of datatype: "nested column branch had multiple children"
        for col in df.columns:
            weird = (df[[col]].applymap(type) !=
                     df[[col]].iloc[0].apply(type)).any(axis=1)
            if len(df[weird]) > 0:
                df[col] = df[col].astype(str)
            if df[col].dtype == list:
                df[col] = df[col].astype(str)
        # write parquet file
        df.to_parquet(self.output().path, engine='auto', compression='snappy')

class Task_40_metaPreproc(luigi.task.WrapperTask):
    '''
    Guardar los metadatos de la descarga de datos del schema RAW
    Son guardados en la base de datos nyc311_metadata en la tabla raw.etl_execution
    '''
    # ==============================
    # parametros:
    # ==============================
    bucket = luigi.Parameter(default="prueba-nyc311")
    year = luigi.Parameter()
    month = luigi.Parameter()
    day = luigi.Parameter()
    # ==============================

    def requires(self):
        return Task_30_preproc(year=self.year, month=self.month, day=self.day)

    def run(self):
        # se instancia la clase raw_metadata()
        cwd = os.getcwd()  # directorio actual
        raw_prep = preproc_metadata()
        raw_prep.name = f"data_{self.year}_{self.month}_{self.day}"
        raw_prep.user = str(getpass.getuser())
        raw_prep.machine = str(platform.platform())
        raw_prep.ip = execv("curl ipecho.net/plain ; echo", cwd)
        raw_prep.creation_date = str(datetime.datetime.now())
        raw_prep.location = f"{path_raw}/{raw_prep.name}"
        raw_prep.param_year = str(self.year)
        raw_prep.param_month = str(self.month)
        raw_prep.param_day = str(self.day)
        raw_prep.param_bucket = str(self.bucket)

        ubicacion_completa = f"{raw_prep.location}.json"
        meta = raw_prep.info()  # extraer información de la clase

        print("=" * 100)
        print(meta)
        print("complete name: ", ubicacion_completa)
        print("name: ", raw_prep.name)
        print("extensión: ", raw_prep.extention)
        print("tamaño: ", raw_prep.size)
        print("action: ", raw_prep.action)
        print("usuario: ", raw_prep.user)
        print("maquina: ", raw_prep.machine)
        print("ip: ", raw_prep.ip)
        print("fecha de creación: ", raw_prep.creation_date)
        print("ubicación: ", raw_prep.location)
        print("param [year]: ", raw_prep.param_year)
        print("param [month]: ", raw_prep.param_month)
        print("param [day]: ", raw_prep.param_day)
        print("param [bucket]: ", raw_prep.param_bucket)
        print("=" * 100)

        # conectarse a la base de datos y guardar a esquema raw.etl_execution
        conn = ps.connect(host=settings.get('host'),
                          port=settings.get('port'),
                          database="nyc311_metadata",
                          user=settings.get('usr'),
                          password=settings.get('password'))
        cur = conn.cursor()
        columns = "(name, extention, schema, action, creator, machine, ip, creation_date, size, location,status, param_year, param_month, param_day, param_bucket)"
        sql = "INSERT INTO preprocessed.etl_execution" + columns + \
            " VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);"

        cur.execute(sql, meta)
        conn.commit()
        cur.close()
        conn.close()

class Task_50_cleaned(luigi.Task):
    '''
    Limpiar los datos que se tienen en parquet utilizando pandas
    '''
    # ==============================
    # parametros:
    # ==============================
    bucket = luigi.Parameter(default="prueba-nyc311")
    year = luigi.Parameter()
    month = luigi.Parameter()
    day = luigi.Parameter()
    # ==============================
    def requires(self):
        return Task_40_metaPreproc(year=self.year, month=self.month, day=self.day)

    def output(self):
        # guarda los datos en s3://prueba-nyc311/raw/.3..
        output_path = f"s3://{self.bucket}/cleaned/{self.year}/{self.month}/{self.day}/data_{self.year}_{self.month}_{self.day}.parquet"
        return luigi.contrib.s3.S3Target(path=output_path)


    def run(self):
        import functionsV1 as f1
        import io

        # Autenticación en S3
        ses = boto3.session.Session(
            profile_name='luigi_dpa', region_name='us-west-2')
        buffer=io.BytesIO()
        s3_resource = ses.resource('s3')
        obj = s3_resource.Bucket(name=self.bucket)

        #lectura de datos
        key = f"preprocess/{self.year}/{self.month}/{self.day}/data_{self.year}_{self.month}_{self.day}.parquet"
        parquet_object = s3_resource.Object(bucket_name=self.bucket, key=key) # objeto
        data_parquet_object = io.BytesIO(parquet_object.get()['Body'].read())#.decode() # info del objeto
        df = pd.read_parquet(data_parquet_object)
        df=f1.to_cleaned(df)

        print(df["agency_name"])
        print(df.head())
        print(df.shape)
        #pasa a formato parquet
        df.to_parquet(self.output().path, engine='auto', compression='snappy')
