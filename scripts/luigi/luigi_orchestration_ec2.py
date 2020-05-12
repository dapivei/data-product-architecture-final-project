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
import numpy as np
import pickle

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

class cleaned_metadata():
    def __init__(self,
                 name="",
                 extention="parquet",
                 schema="cleaned",
                 action="clean parquet",
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

        #pasa a formato parquet
        df.to_parquet(self.output().path, engine='auto', compression='snappy')


class Task_51_cleaned_test(luigi.Task):
    bucket = luigi.Parameter(default="prueba-nyc311")
    year = luigi.Parameter()
    month = luigi.Parameter()
    day = luigi.Parameter()

    def requires(self):
        return Task_50_cleaned(year=self.year, month=self.month, day=self.day)

    def output(self):
        output_path = f"s3://{self.bucket}/cleaned/{self.year}/{self.month}/{self.day}/unit_test_ok.txt"
        return luigi.contrib.s3.S3Target(path=output_path)

    def run(self):
        import subprocess
        args = list(map(str, ["./unit_test/run_clean_test.sh",self.day,self.month,self.year,self.bucket]))
        proc = subprocess.Popen(args)
        proc.wait()

        success = proc.returncode == 0
        if not success:
            import sys
            sys.tracebacklimit=0
            raise TypeError("\n Preuba Fallida \n")


        out=open('clean_test_output.txt','r').read()
        with self.output().open('w') as output_file:
            output_file.write(out)


class Task_60_metaClean(luigi.task.WrapperTask):
    '''
    Guardar los metadatos de la descarga de datos del schema cleaned
    Son guardados en la base de datos nyc311_metadata en la tabla clean.etl_execution
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
        return Task_50_cleaned(year=self.year, month=self.month, day=self.day)

    def run(self):
        # se instancia la clase raw_metadata()
        cwd = os.getcwd()  # directorio actual
        raw_prep = cleaned_metadata()
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
        sql = "INSERT INTO cleaned.etl_execution" + columns + \
            " VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);"

        cur.execute(sql, meta)
        conn.commit()
        cur.close()
        conn.close()


class Task_70_mlPreproc(luigi.Task):
    '''
    Contar los registros por fecha y colapsar en una sola tabla que contendra las columnas de created_date y numero de registros
    '''
    # ==============================
    # parametros:
    # ==============================
    bucket = luigi.Parameter(default="prueba-nyc311")
    year = luigi.Parameter()
    month = luigi.Parameter()
    day = luigi.Parameter()
    parte = luigi.Parameter()
    # ==============================
    def requires(self):
        return Task_60_metaClean(year=self.year, month=self.month, day=self.day)

    def input(self):
        '''
        Acá se lee el dataframe input diario
        '''
        import io
        # Autenticación en S3
        ses = boto3.session.Session(
            profile_name='luigi_dpa', region_name='us-west-2')
        buffer=io.BytesIO()
        s3_resource = ses.resource('s3')
        obj = s3_resource.Bucket(name=self.bucket)
        #lectura de datos
        #key = f"mlpreproc/mlPreproc_until_part{part_num-1}.parquet"
        key = f"cleaned/{self.year}/{self.month}/{self.day}/data_{self.year}_{self.month}_{self.day}.parquet"
        parquet_object = s3_resource.Object(bucket_name=self.bucket, key=key) # objeto
        data_parquet_object = io.BytesIO(parquet_object.get()['Body'].read())
        df = pd.read_parquet(data_parquet_object)
        df=df.loc[(df["agency"]=='nypd') & (df["complaint_type"].str.contains("noise")),:]
        df=df.reset_index(drop=True)
        return df

    def output(self):
        # guarda los datos en s3://prueba-nyc311/mlpreproc/...
        part_num = int(self.parte)
        output_path = f"s3://{self.bucket}/mlpreproc/mlPreproc_until_part{part_num}.parquet"
        # output_path = f"s3://{self.bucket}/mlpreproc/mlPreproc_until_part1.parquet"
        return luigi.contrib.s3.S3Target(path=output_path)

    def run(self):
        import datetime
        year_num = int(self.year)
        month_num = int(self.month)
        day_num = int(self.month)
        date = datetime.datetime(year_num, month_num, day_num)
        day_of_year = (date - datetime.datetime(year_num, 1, 1)).days + 1

        print(day_of_year)
        #cuenta los registros y colapsa el df
        df = self.input()
        df['counts']=1
        df=df.loc[:,['created_date','counts']]
        df=df.groupby(['created_date']).count()
        print(df.head())

        # Autenticación en S3
        import io
        ses = boto3.session.Session(
            profile_name='luigi_dpa', region_name='us-west-2')
        buffer=io.BytesIO()
        s3_resource = ses.resource('s3')
        obj = s3_resource.Bucket(name=self.bucket)
        # acá leemos los
        part_num = int(self.parte)
        key = f"mlpreproc/mlPreproc_until_part{part_num-1}.parquet"
        parquet_object = s3_resource.Object(bucket_name=self.bucket, key=key)
        data_parquet_object = io.BytesIO(parquet_object.get()['Body'].read())
        df2 = pd.read_parquet(data_parquet_object)

        print("="*50)
        print("segundo dataframe ingresado")
        print(day_of_year)
        print(df.head())
        print(df.shape)
        print(df2.head())
        print(df2.shape)
        print("="*50)

        # append los dataframes
        joined_df=df2.append(df)
        joined_df.drop_duplicates(inplace=True)

        print("*"*50)
        print("*"*50)
        print("*** Appended dataframe ***")
        print(joined_df.head())
        print("*"*50)
        print("*"*50)
        # #pasa a formato parquet
        joined_df.to_parquet(self.output().path, engine='auto', compression='snappy')
        #df.to_parquet(self.output().path, engine='auto', compression='snappy')

        #table = pa.Table.from_pandas(df)
        #s3 = fs.S3FileSystem(region='us-west-2')
        # Write direct to your parquet file
        #output_path = f"s3://{self.bucket}/mlpreproc/mlPreproc.parquet"
        #pq.write_to_dataset(table , root_path=output_path,filesystem=s3)


class Task_71_mlPreproc_firstTime(luigi.Task):
    '''
    Contar los registros por fecha y colapsar en una sola tabla que contendra las columnas de created_date y numero de registros
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
        return Task_60_metaClean(year=self.year, month=self.month, day=self.day)

    def output(self):
        # guarda los datos en s3://prueba-nyc311/raw/.3..
        output_path = f"s3://{self.bucket}/mlpreproc/mlPreproc.parquet"
        return luigi.contrib.s3.S3Target(path=output_path)

    def run(self):
        import io
        from datetime import datetime, timedelta
        # Autenticación en S3
        ses = boto3.session.Session(
            profile_name='luigi_dpa', region_name='us-west-2')
        buffer=io.BytesIO()
        s3_resource = ses.resource('s3')
        obj = s3_resource.Bucket(name=self.bucket)

        end_date=datetime(int(self.year),int(self.month),int(self.day))
        start_date= datetime(2009,12,31)
        date=start_date

        flag=0
        count=0
        while(date<end_date):
            date=date+timedelta(days=1)

            #lectura de datos
            try:
                key = f"cleaned/{date.year}/{date.month}/{date.day}/data_{date.year}_{date.month}_{date.day}.parquet"
                parquet_object = s3_resource.Object(bucket_name=self.bucket, key=key) # objeto
                data_parquet_object = io.BytesIO(parquet_object.get()['Body'].read())
                df = pd.read_parquet(data_parquet_object)
            except:
                #para generar metadata
                print(date)
                continue

            # Filtramos los casos de ruido para la agencia NYPD
            df=df.loc[(df["agency"]=='nypd') & (df["complaint_type"].str.contains("noise")),:]
            df=df.reset_index(drop=True)

            #cuenta los registros y colapsa el df
            df['counts']=1
            df=df.loc[:,['created_date','counts','borough']]
            df=df.groupby(['created_date','borough'],as_index=False).count()

            #create or append df
            if(flag==0):
                df2=df
                flag=1
            else:
                #pegamos los dataframes
                df2=df2.append(df)

            del(df)

            #keep track of progress
            count=count+1
            if(count%100==0):
                print(count)
            #aumentamos un dia

            #print(date)

        df2.drop_duplicates(inplace=True)
        df2=df2.reset_index(drop=True)
        #print(df2)
        #pasa a formato parquet
        df2.to_parquet(self.output().path, engine='auto', compression='snappy')



class Task_80_metaMlPreproc(luigi.task.WrapperTask):
    '''
    Guardar los metadatos de mlPreproc
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
        return Task_71_mlPreproc_firstTime(year=self.year, month=self.month, day=self.day)

    def run(self):
        # se instancia la clase raw_metadata()
        cwd = os.getcwd()  # directorio actual
        raw_meta = mlPreproc_metadata()
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
        sql = "INSERT INTO mlpreproc.feature_engineering" + columns + \
            " VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);"
        cur.execute(sql, meta)
        conn.commit()
        cur.close()
        conn.close()

class Task_91_ml_firstTime(luigi.Task):
    '''
    Contar los registros por fecha y colapsar en una sola tabla que contendra las columnas de created_date y numero de registros
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
        return Task_71_mlPreproc_firstTime(year=self.year, month=self.month, day=self.day)

    def output(self):
        # guarda los datos en s3://prueba-nyc311/raw/.3..
        output_path = f"s3://{self.bucket}/ml/ml.parquet"
        return luigi.contrib.s3.S3Target(path=output_path)

    def run(self):
        import functionsV1 as f1
        import io
        import numpy as np


        # Autenticación en S3
        ses = boto3.session.Session(profile_name='luigi_dpa', region_name='us-west-2')
        buffer=io.BytesIO()
        s3_resource = ses.resource('s3')
        obj = s3_resource.Bucket(name=self.bucket)

        #lectura de datos
        key = f"mlpreproc/mlPreproc.parquet"
        parquet_object = s3_resource.Object(bucket_name=self.bucket, key=key) # objeto
        data_parquet_object = io.BytesIO(parquet_object.get()['Body'].read())
        df = pd.read_parquet(data_parquet_object)

        # funcion de procesamiento de datos
        df_features=f1.create_feature_table(df)
        del(df)
        print(df_features)
        #falta pasar a un solo parquet que vaya haciendo append de las fechas
        #pasa a formato parquet
        df_features.to_parquet(self.output().path, engine='auto', compression='snappy')

class Task_100_Train(luigi.Task):
    '''
    Entrena un modelo
    '''
    # ==============================
    # parametros:
    # ==============================
    bucket = luigi.Parameter(default="prueba-nyc311")
    nestimators =luigi.Parameter()
    maxdepth= luigi.Parameter()
    criterion=luigi.Parameter()
    year = luigi.Parameter(default='2020')
    month = luigi.Parameter(default='2')
    day = luigi.Parameter(default='2')


    # ==============================
    def requires(self):
        return Task_91_ml_firstTime(year=self.year, month=self.month, day=self.day)
        #return Task_71_mlPreproc_firstTime('2020', '1', '1')

    def output(self):
        output_path = f"s3://{self.bucket}/ml/modelos/depth{self.maxdepth}_{self.criterion}_estimatros{self.nestimators}.pickle"
        return luigi.contrib.s3.S3Target(path=output_path)

    def run(self):
        import functionsV1 as f1
        import io
        import numpy as np
        from sklearn.model_selection import TimeSeriesSplit
        from sklearn.ensemble import RandomForestClassifier

        # Autenticación en S3
        ses = boto3.session.Session(profile_name='luigi_dpa', region_name='us-west-2')
        buffer=io.BytesIO()
        s3_resource = ses.resource('s3')
        obj = s3_resource.Bucket(name=self.bucket)

        #lectura de datos
        key = f"ml/ml.parquet"
        parquet_object = s3_resource.Object(bucket_name=self.bucket, key=key) # objeto
        data_parquet_object = io.BytesIO(parquet_object.get()['Body'].read())
        df = pd.read_parquet(data_parquet_object)

        # output variable
        y=df["mean_flag"]
        df2=df.drop(columns=["mean_flag","created_date"])

        #separamos los primeros 70% de los datos para entrenar
        X_train = df2[:int(df2.shape[0]*0.7)].to_numpy()
        X_test = df2[int(df2.shape[0]*0.7):].to_numpy()
        y_train = y[:int(df2.shape[0]*0.7)].to_numpy()
        y_test = y[int(df2.shape[0]*0.7):].to_numpy()

        #partimos los datos con temporal cv
        tscv=TimeSeriesSplit(n_splits=5)
        for tr_index, val_index in tscv.split(X_train):
            X_tr, X_val=X_train[tr_index], X_train[val_index]
            y_tr, y_val = y_train[tr_index], y_train[val_index]

        #Define y entrena el modelo
        model=RandomForestClassifier(max_depth=int(self.maxdepth),criterion=self.criterion,n_estimators=int(self.nestimators),n_jobs=-1)
        model.fit(X_tr,y_tr)


        #genera el pickle
        pick=open('nombre.pickle','wb')
        pickle.dump(model,pick)
        pick.close()

        #llama a output
        with self.output().open('w') as output_file:
           output_file.write("nombre.pickle")

###################################################################
# clase y tarea de guardado de metadatos de modelado
class model_metadata():
    def __init__(self,
                 model_name="",
                 model_type="sklearn model",
                 schema="modelling",
                 action="ML training model",
                 creator="-",
                 machine="",
                 ip="",
                 date="",
                 location="",
                 status="sucess",
                 max_depth="",
                 criterion="",
                 n_estimators="",
                 score_train=""):

        # asignamos las características de los metadatos
        self.model_name = model_name
        self.model_type = model_type
        self.schema = schema
        self.action = action
        self.creator = creator
        self.machine = machine
        self.ip = ip
        self.date = date
        self.location = location
        self.status = status
        self.max_depth = max_depth
        self.criterion = criterion
        self.n_estimators = n_estimators
        self.score_train = score_train

    def info(self):
        return (self.model_name, self.model_type, self.schema, self.action,
                self.creator, self.machine, self.ip, self.date, self.location,
                self.status, self.max_depth, self.criterion, self.n_estimators,
                self.score_train)




class Task_110_metaModel(luigi.task.WrapperTask):
    '''
    Guardar los metadatos del entrenamiento de modelos
    '''
    # ==============================
    # parametros:
    # ==============================
    bucket = luigi.Parameter(default="prueba-nyc311")
    nestimators =luigi.Parameter()
    maxdepth= luigi.Parameter()
    criterion=luigi.Parameter()
    #year = luigi.Parameter()
    #month = luigi.Parameter()
    #day = luigi.Parameter()


    def requires(self):
        return Task_100_Train(nestimators=self.nestimators, maxdepth=self.maxdepth, criterion=self.criterion)
        #return luigi.contrib.s3.exist(year=self.year, month=self.month, day=self.day)
        #return luigi.S3Target(f"s3://{self.bucket}/ml/ml.parquet")

    def run(self):
        import os
    # ==============================
    # se instancia la clase raw_metadata()
        cwd = os.getcwd()  # directorio actual
        model_meta = model_metadata()
        model_meta.model_name = f"depth{self.maxdepth}_{self.criterion}_estimatros{self.nestimators}.pickle"
        model_meta.creator = str(getpass.getuser())
        model_meta.machine = str(platform.platform())
        model_meta.ip = execv("curl ipecho.net/plain ; echo", cwd)
        model_meta.date = str(datetime.datetime.now())
        model_meta.location = f"s3://{self.bucket}/ml/modelos/depth{self.maxdepth}_{self.criterion}_estimatros{self.nestimators}.pickle"
        model_meta.max_depth = str(self.maxdepth)
        model_meta.criterion = str(self.criterion)
        model_meta.n_estimators = str(self.nestimators)


        ubicacion_completa = model_meta.location
        meta = model_meta.info()  # extrae info de la clas

        # conectarse a la base de datos y guardar a esquema raw.etl_execution
        conn = ps.connect(host=settings.get('host'),
                          port=settings.get('port'),
                          database="nyc311_metadata",
                          user=settings.get('usr'),
                          password=settings.get('password'))
        cur = conn.cursor()
        columns = "(model_name, model_type, schema, action, creator, machine, ip, date, location, status, max_depth, criterion, n_estimators, score_train)"
        sql = "INSERT INTO modeling.ejecucion" + columns + \
            " VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);"
        cur.execute(sql, meta)
        conn.commit()
        cur.close()
        conn.close()
