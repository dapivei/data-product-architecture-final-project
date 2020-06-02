#!/usr/bin/env python
# -*- coding: utf-8 -*-
# some_file.py
#import sys
#sys.path.insert(0, '/tasks')
#from tasks import Task_70
#import tasks.Task_70

import json
import boto3
import botocore
import datetime
import getpass
import luigi
import luigi.contrib.s3

import os
import io
import pandas as pd
import platform
import psycopg2 as ps
import pyarrow as pa
import socket
import numpy as np
import pickle
import s3fs

from classes import *
from luigi.contrib.postgres import CopyToTable
from functionsV2 import queryApi311
from functionsV2 import execv

from datetime import date
from dynaconf import settings
from luigi.contrib.s3 import S3Client, S3Target
from sodapy import Socrata

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


class Task_11_metaDownload(CopyToTable):
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
    database = 'nyc311_metadata'
    host = settings.get('host')
    user = settings.get('usr')
    password = settings.get('password')
    table = 'raw.etl_execution'
    columns = [("name","TEXT"), ("extention","TEXT") , ("schema","TEXT"),
            ("action","TEXT") , ("creator","TEXT"), ("machine","TEXT"),
            ("ip","TEXT"), ("creation_date","TEXT"), ("size","TEXT"),
            ("location","TEXT"),("status","TEXT"), ("param_year","TEXT"),
            ("param_month","TEXT"), ("param_day","TEXT"), ("param_bucket","TEXT")]

    def requires(self):
        return Task_10_download(year=self.year, month=self.month, day=self.day)

    def rows(self):
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
        yield (meta)


class Task_20_preproc(luigi.Task):
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
        return Task_11_metaDownload(year=self.year, month=self.month, day=self.day)

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

class Task_21_metaPreproc(CopyToTable):
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
    database = 'nyc311_metadata'
    host = settings.get('host')
    user = settings.get('usr')
    password = settings.get('password')
    table = 'preprocessed.etl_execution'
    columns = [("name","TEXT"), ("extention","TEXT") , ("schema","TEXT"),
            ("action","TEXT") , ("creator","TEXT"), ("machine","TEXT"),
            ("ip","TEXT"), ("creation_date","TEXT"), ("size","TEXT"),
            ("location","TEXT"),("status","TEXT"), ("param_year","TEXT"),
            ("param_month","TEXT"), ("param_day","TEXT"), ("param_bucket","TEXT")]

    def requires(self):
        return Task_20_preproc(year=self.year, month=self.month, day=self.day)

    def rows(self):
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
        yield (meta)


class Task_30_cleaned(luigi.Task):
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
        return Task_21_metaPreproc(year=self.year, month=self.month, day=self.day)

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

class Task_31_metaClean(CopyToTable):
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
    database = 'nyc311_metadata'
    host = settings.get('host')
    user = settings.get('usr')
    password = settings.get('password')
    table = 'cleaned.etl_execution'
    columns = [("name","TEXT"), ("extention","TEXT") , ("schema","TEXT"),
            ("action","TEXT") , ("creator","TEXT"), ("machine","TEXT"),
            ("ip","TEXT"), ("creation_date","TEXT"), ("size","TEXT"),
            ("location","TEXT"),("status","TEXT"), ("param_year","TEXT"),
            ("param_month","TEXT"), ("param_day","TEXT"), ("param_bucket","TEXT")]

    def requires(self):
        return Task_30_cleaned(year=self.year, month=self.month, day=self.day)

    def rows(self):
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
        yield (meta)

class Task_32_cleaned_UnitTest(luigi.Task):
    bucket = luigi.Parameter(default="prueba-nyc311")
    year = luigi.Parameter()
    month = luigi.Parameter()
    day = luigi.Parameter()

    def requires(self):
        return Task_31_metaClean(year=self.year, month=self.month, day=self.day)

    def output(self):
        output_path = f"s3://{self.bucket}/cleaned/{self.year}/{self.month}/{self.day}/unit_test_ok"
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
            raise TypeError("\n Prueba Fallida \n")

        out=open('clean_test_output.txt','r').read()
        with self.output().open('w') as output_file:
            output_file.write(out)
# En caso de éxito guarda metadatos, de otra forma no.
@Task_32_cleaned_UnitTest.event_handler(luigi.Event.SUCCESS)
def celebrate_success(task):
    print(u'\u2705'*1, "UnitTest con Marbles para schema Cleaned Task completado. Se procede a guardar los metadatos.")
@Task_32_cleaned_UnitTest.event_handler(luigi.Event.FAILURE)
def mourn_failure(task, exception):
    print(u'\u274C'*1, "UnitTest con Marbles para schema Cleaned Task fallido. No se guardan los metadatos.")


class Task_33_metaCleanUT(CopyToTable):
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
    database = 'nyc311_metadata'
    host = settings.get('host')
    user = settings.get('usr')
    password = settings.get('password')
    table = 'cleaned.ut_execution'
    columns = [("name","TEXT"), ("extention","TEXT") , ("schema","TEXT"),
            ("action","TEXT") , ("creator","TEXT"), ("machine","TEXT"),
            ("ip","TEXT"), ("creation_date","TEXT"), ("size","TEXT"),
            ("location","TEXT"),("status","TEXT"), ("param_year","TEXT"),
            ("param_month","TEXT"), ("param_day","TEXT"), ("param_bucket","TEXT")]

    def requires(self):
        return Task_32_cleaned_UnitTest(year=self.year, month=self.month, day=self.day)

    def rows(self):
        cwd = os.getcwd()  # directorio actual
        cleanUT = cleaned_metadataUnitTest()
        cleanUT.name = f"data_{self.year}_{self.month}_{self.day}"
        cleanUT.user = str(getpass.getuser())
        cleanUT.machine = str(platform.platform())
        cleanUT.ip = execv("curl ipecho.net/plain ; echo", cwd)
        cleanUT.creation_date = str(datetime.datetime.now())
        cleanUT.location = f"{path_raw}/{cleanUT.name}"
        cleanUT.param_year = str(self.year)
        cleanUT.param_month = str(self.month)
        cleanUT.param_day = str(self.day)
        cleanUT.param_bucket = str(self.bucket)

        ubicacion_completa = f"{cleanUT.location}.json"
        meta = cleanUT.info()  # extraer información de la clase
        yield (meta)


class Task_40_mlPreproc(luigi.Task):
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
        return Task_33_metaCleanUT(year=self.year, month=self.month, day=self.day)

    def output(self):
        output_path = f"s3://{self.bucket}/mlPreproc/{self.year}/{self.month}/{self.day}/data_{self.year}_{self.month}_{self.day}.parquet"
        return luigi.contrib.s3.S3Target(path=output_path)

    def write(self,date):
        # guarda los datos en s3://prueba-nyc311/raw/.3..
        output_path = f"s3://{self.bucket}/mlPreproc/{date.year}/{date.month}/{date.day}/data_{date.year}_{date.month}_{date.day}.parquet"
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

        #para que solamente baje los datos de la fecha indicada
        date=end_date-timedelta(days=1)

        flag=0
        count=0
        while(date<=end_date):
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


            df.drop_duplicates(inplace=True)
            df=df.reset_index(drop=True)


            df.to_parquet(self.write(date).path, engine='auto', compression='snappy')
            if(date==end_date):
                df.to_parquet(self.output().path, engine='auto', compression='snappy')

            del(df)

            #keep track of progress
            count=count+1
            if(count%100==0):
                print(count)


class Task_50_ml(luigi.Task):
    '''
    Genera la matriz de machine learning
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
        return Task_40_mlPreproc(year=self.year, month=self.month, day=self.day)

    def output(self):
        output_path = f"s3://{self.bucket}/ml/{self.year}/{self.month}/{self.day}/data_{self.year}_{self.month}_{self.day}.parquet"
        return luigi.contrib.s3.S3Target(path=output_path)

    def run(self):
        import functionsV1 as f1
        import io
        import numpy as np
        from datetime import datetime, timedelta

        # Autenticación en S3
        ses = boto3.session.Session(profile_name='luigi_dpa', region_name='us-west-2')
        buffer=io.BytesIO()
        s3_resource = ses.resource('s3')
        obj = s3_resource.Bucket(name=self.bucket)

        #inicio fechas
        end_date=datetime(int(self.year),int(self.month),int(self.day))
        start_date= datetime(2009,12,31)
        date=start_date

        flag=0
        count=0
        while(date<end_date):
            date=date+timedelta(days=1)

            try:
                #lectura de datos
                key = f"mlPreproc/{date.year}/{date.month}/{date.day}/data_{date.year}_{date.month}_{date.day}.parquet"
                parquet_object = s3_resource.Object(bucket_name=self.bucket, key=key) # objeto
                data_parquet_object = io.BytesIO(parquet_object.get()['Body'].read())
                df = pd.read_parquet(data_parquet_object)
            except:
                #para generar metadata
                print(date)
                continue

            if(flag==0):
                df2=df
                flag=1

            else: df2=df2.append(df)
        # funcion de procesamiento de datos
        df_features=f1.create_feature_table(df2)
        del(df)
        df_features=f1.encoders(df_features)
        #print(df_features)

        df_features.to_parquet(self.output().path, engine='auto', compression='snappy')

class Task_51_feature_UnitTest(luigi.task.WrapperTask):
    '''
    Realiza 2 test unitarios con marbles a ml.parquet:
    test_created_date_year_vs_onehot & test_created_date_month_vs_onehot.
    En caso de exito guarda el archivo s3: prueba-nyc311/ml/ut_FE_marbles_ok
    '''
    bucket = luigi.Parameter(default="prueba-nyc311")
    year = luigi.Parameter()
    month = luigi.Parameter()
    day = luigi.Parameter()

    def requires(self):
        return Task_50_ml(year=self.year, month=self.month, day=self.day)

    def run(self):
        import subprocess
        args = list(map(str, ["./unit_test/run_feature_test.sh",self.bucket]))
        proc = subprocess.Popen(args)
        proc.wait()

        success = proc.returncode == 0
        if not success:
            import sys
            sys.tracebacklimit=0
            raise TypeError("\n Prueba Fallida \n")

# En caso de éxito guarda metadatos, de otra forma no.
@Task_51_feature_UnitTest.event_handler(luigi.Event.SUCCESS)
def celebrate_success(task):
    print(u'\u2705'*1, "UnitTest con Marbles para schema Feature Engineering Task completado. Se procede a guardar los metadatos.")
@Task_51_feature_UnitTest.event_handler(luigi.Event.FAILURE)
def mourn_failure(task, exception):
    print(u'\u274C'*1, "UnitTest con Marbles para schema Feature Engineering Task fallido. No se guardan los metadatos.")



class Task_52_metaFeatureEngUTM(CopyToTable):
    '''
    Guardar los metadatos de la descarga de datos del schema FE con marbles
    Son guardados en la base de datos nyc311_metadata en la tabla mlpreproc.ut_execution
    '''
    # ==============================
    # parametros:
    # ==============================
    bucket = luigi.Parameter(default="prueba-nyc311")
    year = luigi.Parameter()
    month = luigi.Parameter()
    day = luigi.Parameter()
    # ==============================
    database = 'nyc311_metadata'
    host = settings.get('host')
    user = settings.get('usr')
    password = settings.get('password')
    table = 'mlpreproc.ut_execution'
    columns = [("name","TEXT"), ("extention","TEXT") , ("schema","TEXT"),
            ("action","TEXT") , ("creator","TEXT"), ("machine","TEXT"),
            ("ip","TEXT"), ("creation_date","TEXT"), ("size","TEXT"),
            ("location","TEXT"),("status","TEXT"), ("param_bucket","TEXT")]

    def requires(self):
        return Task_51_feature_UnitTest(year=self.year, month=self.month, day=self.day)

    def rows(self):
        # se instancia la clase raw_metadata()
        cwd = os.getcwd()  # directorio actual
        feUT = FE_metadataUnitTest()
        feUT.name = "ml"
        feUT.user = str(getpass.getuser())
        feUT.machine = str(platform.platform())
        feUT.ip = execv("curl ipecho.net/plain ; echo", cwd)
        feUT.creation_date = str(datetime.datetime.now())
        feUT.location = "ml/ml.parquet"
        feUT.param_bucket = str(self.bucket)
        # las pruebas unitarias que se superaron
        feUT.action = "unit test for feature engineering (marbles): test_created_date_year_vs_onehot & test_created_date_month_vs_onehot"

        ubicacion_completa = f"{feUT.location}.parquet"
        meta = feUT.info()  # extraer información de la clase

        yield (meta)
@Task_52_metaFeatureEngUTM.event_handler(luigi.Event.SUCCESS)
def celebrate_success(task):
    print(u'\u2705'*2, "Se guardaron los metadatos para UT con marbles.")


class Task_53_feature_PandasTest(luigi.task.WrapperTask):
    bucket = luigi.Parameter(default="prueba-nyc311")
    year = luigi.Parameter()
    month = luigi.Parameter()
    day = luigi.Parameter()

    def requires(self):
        return Task_50_ml(year=self.year, month=self.month, day=self.day)

    def run(self):
        import sys
        from functionsV1 import NumberCases
        import io
        # Autenticación en S3
        ses = boto3.session.Session(profile_name='luigi_dpa', region_name='us-west-2')
        buffer=io.BytesIO()
        s3_resource = ses.resource('s3')
        obj = s3_resource.Bucket(name=self.bucket)

        #lectura de datos
        key = f"ml/{self.year}/{self.month}/{self.day}/data_{self.year}_{self.month}_{self.day}.parquet"
        parquet_object = s3_resource.Object(bucket_name=self.bucket, key=key) # objeto
        data_parquet_object = io.BytesIO(parquet_object.get()['Body'].read())
        df = pd.read_parquet(data_parquet_object)

        NumberCases.prueba_casos_dia(NumberCases,df)

# En caso de éxito guarda metadatos, de otra forma no.
@Task_53_feature_PandasTest.event_handler(luigi.Event.SUCCESS)
def celebrate_success(task):
    print(u'\u2705'*1,"UnitTest con Pandas para schema Feature Engineering Task completado. Se procede a guardar los metadatos.")
@Task_53_feature_PandasTest.event_handler(luigi.Event.FAILURE)
def mourn_failure(task, exception):
    print(u'\u274C'*1, "UnitTest con Pandas para schema Feature Engineering Task fallido. No se guardan los metadatos.")

class Task_54_metaFeatureEngUTP(CopyToTable):
    '''
    Guardar los metadatos de la descarga de datos del schema FE para unit testing con pandas
    Son guardados en la base de datos nyc311_metadata en la tabla mlpreproc.ut_execution
    '''
    # ==============================
    # parametros:
    # ==============================
    bucket = luigi.Parameter(default="prueba-nyc311")
    year = luigi.Parameter()
    month = luigi.Parameter()
    day = luigi.Parameter()
    # ==============================
    database = 'nyc311_metadata'
    host = settings.get('host')
    user = settings.get('usr')
    password = settings.get('password')
    table = 'mlpreproc.ut_execution'
    columns = [("name","TEXT"), ("extention","TEXT") , ("schema","TEXT"),
            ("action","TEXT") , ("creator","TEXT"), ("machine","TEXT"),
            ("ip","TEXT"), ("creation_date","TEXT"), ("size","TEXT"),
            ("location","TEXT"),("status","TEXT"), ("param_bucket","TEXT")]

    def requires(self):
        return Task_53_feature_PandasTest(year=self.year, month=self.month, day=self.day)

    def rows(self):
        # se instancia la clase raw_metadata()
        cwd = os.getcwd()  # directorio actual
        feUT = FE_metadataUnitTest()
        feUT.name = "ml"
        feUT.user = str(getpass.getuser())
        feUT.machine = str(platform.platform())
        feUT.ip = execv("curl ipecho.net/plain ; echo", cwd)
        feUT.creation_date = str(datetime.datetime.now())
        feUT.location = "ml/ml.parquet"
        feUT.param_bucket = str(self.bucket)
        # las pruebas unitarias que se superaron
        feUT.action = "unit test for feature engineering (marbles): test_created_date_year_vs_onehot & test_created_date_month_vs_onehot"

        ubicacion_completa = f"{feUT.location}.parquet"
        meta = feUT.info()  # extraer información de la clase

        yield (meta)
@Task_54_metaFeatureEngUTP.event_handler(luigi.Event.SUCCESS)
def celebrate_success(task):
    print(u'\u2705'*2, "Se guardaron los metadatos para UT con pandas.")


class Task_60_Train(luigi.Task):
    '''
    Entrena un modelo
    '''
    # ==============================
    # parametros:
    # ==============================
    bucket = luigi.Parameter(default="prueba-nyc311")
    nestimators =luigi.Parameter(default=15)
    maxdepth= luigi.Parameter(default=20)
    criterion=luigi.Parameter(default='gini')
    year = luigi.Parameter()
    month = luigi.Parameter()
    day = luigi.Parameter()


    # ==============================
    def requires(self):
        return [
            Task_52_metaFeatureEngUTM(year=self.year, month=self.month, day=self.day),
            Task_54_metaFeatureEngUTP(year=self.year, month=self.month, day=self.day)
        ]

    def output(self):
        output_path = f"s3://{self.bucket}/ml/modelos/{self.criterion}_depth_{self.maxdepth}_estimatros{self.nestimators}_{self.year}_{self.month}_{self.day}.pickle"
        return luigi.contrib.s3.S3Target(path=output_path,format=luigi.format.Nop)

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
        key = f"ml/{self.year}/{self.month}/{self.day}/data_{self.year}_{self.month}_{self.day}.parquet"
        parquet_object = s3_resource.Object(bucket_name=self.bucket, key=key) # objeto
        data_parquet_object = io.BytesIO(parquet_object.get()['Body'].read())
        df = pd.read_parquet(data_parquet_object)

        # output variable
        y=df["mean_flag"]
        df2=df.drop(columns=["mean_flag","created_date","counts","month_mean"])

        #separamos los primeros 70% de los datos para entrenar
        X_train = df2[:int(df2.shape[0]*0.7)].values
        X_test = df2[int(df2.shape[0]*0.7):].values
        y_train = y[:int(df2.shape[0]*0.7)].values
        y_test = y[int(df2.shape[0]*0.7):].values

        #partimos los datos con temporal cv
        tscv=TimeSeriesSplit(n_splits=5)
        for tr_index, val_index in tscv.split(X_train):
            X_tr, X_val=X_train[tr_index], X_train[val_index]
            y_tr, y_val = y_train[tr_index], y_train[val_index]

        #Define y entrena el modelo
        model=RandomForestClassifier(max_depth=int(self.maxdepth),criterion=self.criterion,n_estimators=int(self.nestimators),n_jobs=-1)
        model.fit(X_tr,y_tr)

        from sklearn.metrics import accuracy_score
        print(u'\u2B50'*1)
        y_new= model.predict(X_val)
        print("precisión del modelo validacion", accuracy_score(y_val, y_new))
        print("\n")

        #llama a output
        with self.output().open('w') as output_file:
            pickle.dump(model,output_file)

class Task_61_metaModel(CopyToTable):
    '''
    Guardar los metadatos del entrenamiento de modelos
    '''
    # ==============================
    # parametros:
    # ==============================
    bucket = luigi.Parameter(default="prueba-nyc311")
    nestimators =luigi.Parameter(default=15)
    maxdepth= luigi.Parameter(default=20)
    criterion=luigi.Parameter(default='gini')
    year = luigi.Parameter()
    month = luigi.Parameter()
    day = luigi.Parameter()

    # ==============================
    database = 'nyc311_metadata'
    host = settings.get('host')
    user = settings.get('usr')
    password = settings.get('password')
    table = 'modeling.ejecucion'
    columns = [("model_name","TEXT"), ("model_type","TEXT") , ("schema","TEXT"),
            ("action","TEXT") , ("creator","TEXT"), ("machine","TEXT"),
            ("ip","TEXT"), ("date","TEXT"), ("location","TEXT"),
            ("status","TEXT"), ("max_depth","TEXT"),
            ("criterion","TEXT"), ("n_estimators","TEXT"), ("score_train","TEXT"),
            ("param_year","TEXT"),("param_month","TEXT"), ("param_day","TEXT"),
            ("param_bucket","TEXT")]


    def requires(self):
        return Task_60_Train(nestimators=self.nestimators, maxdepth=self.maxdepth,
                    criterion=self.criterion,year=self.year,month=self.month,day=self.day)

    def rows(self):
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
        model_meta.param_year = str(self.year)
        model_meta.param_day = str(self.day)
        model_meta.param_month = str(self.month)
        model_meta.param_bucket = str(self.bucket)


        ubicacion_completa = model_meta.location
        meta = model_meta.info()  # extrae info de la clas

        yield (meta)

#
class Task_70_biasFairness(luigi.Task):
    '''
    Genera métricas de aequitas para modelo pre-entrenado disponible en S3
    '''
    # ==============================
    # parametros:
    # ==============================
    bucket = luigi.Parameter(default="prueba-nyc311")
    nestimators =luigi.Parameter(default=15)
    maxdepth= luigi.Parameter(default=20)
    criterion=luigi.Parameter(default='gini')
    year = luigi.Parameter(default=2020)
    month = luigi.Parameter(default=4)
    day = luigi.Parameter(default=15)

    def output(self):
        #output_path = f"s3://prueba-nyc311/BiasFairness/aequitas_metricas.csv"
        output_path = f"s3://prueba-nyc311/BiasFairness/aequitas_metricas_RFC_{self.criterion}_depth_{self.maxdepth}_estimatros{self.nestimators}.csv"
        return luigi.contrib.s3.S3Target(path=output_path)

    def requires(self):
        return Task_60_Train(nestimators=self.nestimators, maxdepth=self.maxdepth,
                        criterion=self.criterion,year=self.year,month=self.month,day=self.day)
    def run(self):

        import functionsV1 as f1
        import io
        import numpy as np
        from sklearn.model_selection import TimeSeriesSplit
        from sklearn.ensemble import RandomForestClassifier
        import datetime

        import io
        import aequitas
        from aequitas.group import Group
        from aequitas.preprocessing import preprocess_input_df
        # Autenticación en S3
        ses = boto3.session.Session(profile_name='luigi_dpa', region_name='us-west-2')
        s3_resource = ses.resource('s3')


        #lectura de datos
        key = f"ml/{self.year}/{self.month}/{self.day}/data_{self.year}_{self.month}_{self.day}.parquet"
        parquet_object = s3_resource.Object(bucket_name=self.bucket, key=key) # objeto
        data_parquet_object = io.BytesIO(parquet_object.get()['Body'].read())
        df = pd.read_parquet(data_parquet_object)

        # output variable
        y=df["mean_flag"]
        df2=df.drop(columns=["mean_flag","created_date","counts","month_mean"])

        #separamos los primeros 70% de los datos para entrenar
        X_train = df2[:int(df2.shape[0]*0.7)].values
        X_test = df2[int(df2.shape[0]*0.7):].values
        y_train = y[:int(df2.shape[0]*0.7)].values
        y_test = y[int(df2.shape[0]*0.7):].values

        #partimos los datos con temporal cv
        tscv=TimeSeriesSplit(n_splits=5)
        for tr_index, val_index in tscv.split(X_train):
            X_tr, X_val=X_train[tr_index], X_train[val_index]
            y_tr, y_val = y_train[tr_index], y_train[val_index]

        #### modelo pre-entrenado
        key = f"ml/modelos/{self.criterion}_depth_{self.maxdepth}_estimatros{self.nestimators}_{self.year}_{self.month}_{self.day}.pickle"
        obj = s3_resource.Object(bucket_name=self.bucket, key=key) # objeto
        pkl = obj.get()['Body'].read()
        model=pickle.loads(pkl)
        # inputs de aequitas
        score=model.predict(X_test) # columna con predicciones
        label_value=y_test # columna con etiquetas verdaderas

        pd.set_option('display.max_columns', None)
        pd.set_option('display.max_rows', None)
        # "bronx","brooklyn","manhattan","queens","staten island","undefined"
        bronx_vec =  X_test[:,124]*1
        brooklyn_vec=X_test[:,125]*2
        manhattan_vec=X_test[:,126]*3
        queens_vec =X_test[:,127]*4
        staten_vec =X_test[:,128]*5
        undefined_vec=X_test[:,129]*6
        # se agrega en un solo vector de distrito
        distrito = bronx_vec + brooklyn_vec + manhattan_vec+ queens_vec +staten_vec + undefined_vec
        # crea matriz conformato que requiere aequitas
        aeq_mat = pd.DataFrame({'score': score,
                                'label_value':label_value,
                                'distrito':distrito})
        # crea matriz conformato que requiere aequitas
        preprocess_input_df(aeq_mat)
        g = Group()
        xtab, _ = g.get_crosstabs(aeq_mat)
        #print(xtab)
        # guardar métricas obtenidas
        xtab.to_csv(self.output().path, index=False, encoding='utf-8')


class Task_71_biasFairnessRDS(CopyToTable):
    '''
    Guarda la predicciones a RDS
    '''
    # ==============================
    # parametros:
    # ==============================
    bucket = luigi.Parameter(default="prueba-nyc311")
    nestimators =luigi.Parameter(default=15)
    maxdepth= luigi.Parameter(default=20)
    criterion=luigi.Parameter(default='gini')
    year = luigi.Parameter(default=2020)
    month = luigi.Parameter(default=5)
    day = luigi.Parameter(default=15)

    database = 'nyc311_metadata'
    host = settings.get('host')
    user = settings.get('usr')
    password = settings.get('password')
    table = 'biasfairness.aequitas_ejecucion'
    columns = [("model_name","TEXT"), ("model_id","TEXT") , ("score_threshold","TEXT"),
            ("k","TEXT") , ("attribute_name","TEXT"), ("attribute_value","TEXT"),
            ("tpr","TEXT"), ("tnr","TEXT"), ("fxr","TEXT"),
            ("fdr","TEXT"), ("fpr","TEXT"), ("fnr","TEXT"),
            ("npv","TEXT"), ("precision","TEXT"), ("pp","TEXT"),
            ("pn","TEXT"),("ppr","TEXT"), ("pprev","TEXT"),
            ("fp","TEXT"), ("fn","TEXT"), ("tn","TEXT"), ("tp","TEXT"),
            ("group_label_pos","TEXT"),("group_label_neg","TEXT"),
            ("group_size","TEXT"),("total_entities","TEXT"),
            ("prev","TEXT")]

    def requires(self):
        return Task_70_biasFairness(nestimators=self.nestimators, maxdepth=self.maxdepth,
                        criterion=self.criterion,year=self.year,month=self.month,day=self.day)

    def rows(self):
        import io
        # Autenticación en S3
        ses = boto3.session.Session(profile_name='luigi_dpa', region_name='us-west-2')
        s3_resource = ses.resource('s3')

        #### matriz de datos
        #lectura de datos
        #key = f"BiasFairness/aequitas_metricas.csv"
        key = f"BiasFairness/aequitas_metricas_RFC_{self.criterion}_depth_{self.maxdepth}_estimatros{self.nestimators}.csv"
        parquet_object = s3_resource.Object(bucket_name=self.bucket, key=key) # objeto
        data_csv_object = io.BytesIO(parquet_object.get()['Body'].read())
        sub_df = pd.read_csv(data_csv_object)

        # se instancia la clase raw_metadata()
        bfs_meta = biasFairness_mts()
        bfs_meta.model_name =f"RandomForestClassifier_crit{self.criterion}_depth{self.maxdepth}_estims{self.nestimators}"
        bfs_meta.model_id = sub_df.iloc[0]["model_id"]
        bfs_meta.score_threshold =sub_df.iloc[0]["score_threshold"]
        bfs_meta.k =sub_df.iloc[0]["k"]
        bfs_meta.attribute_name =sub_df.iloc[0]["attribute_name"]
        bfs_meta.attribute_value =sub_df.iloc[0]["attribute_value"]
        bfs_meta.tpr =sub_df.iloc[0]["tpr"]
        bfs_meta.tnr =sub_df.iloc[0]["tnr"]
        bfs_meta.fxr =sub_df.iloc[0]["for"]
        bfs_meta.fdr =sub_df.iloc[0]["fdr"]
        bfs_meta.fpr =sub_df.iloc[0]["fpr"]
        bfs_meta.fnr =sub_df.iloc[0]["fnr"]
        bfs_meta.npv =sub_df.iloc[0]["npv"]
        bfs_meta.precision =sub_df.iloc[0]["precision"]
        bfs_meta.pp =sub_df.iloc[0]["pp"]
        bfs_meta.pn =sub_df.iloc[0]["pn"]
        bfs_meta.ppr =sub_df.iloc[0]["ppr"]
        bfs_meta.pprev =sub_df.iloc[0]["pprev"]
        bfs_meta.fp =sub_df.iloc[0]["fp"]
        bfs_meta.fn =sub_df.iloc[0]["fn"]
        bfs_meta.tn =sub_df.iloc[0]["tn"]
        bfs_meta.tp =sub_df.iloc[0]["tp"]
        bfs_meta.group_label_pos =sub_df.iloc[0]["group_label_pos"]
        bfs_meta.group_label_neg =sub_df.iloc[0]["group_label_neg"]
        bfs_meta.group_size =sub_df.iloc[0]["group_size"]
        bfs_meta.total_entities =sub_df.iloc[0]["total_entities"]
        bfs_meta.prev =sub_df.iloc[0]["prev"]

        data = bfs_meta.info()
        yield (data)

class Task_72_metabiasFairness(CopyToTable):
    '''
    Guardar los metadatos de la generación de métricas de bias y fairness
    Son guardados en la base de datos nyc311_metadata en la tabla raw.etl_execution
    '''
    # ==============================
    # parametros:
    # ==============================
    bucket = luigi.Parameter(default="prueba-nyc311")
    nestimators =luigi.Parameter(default=15)
    maxdepth= luigi.Parameter(default=22)
    criterion=luigi.Parameter(default='gini')
    year = luigi.Parameter(default=2020)
    month = luigi.Parameter(default=5)
    day = luigi.Parameter(default=15)
    # ==============================
    database = 'nyc311_metadata'
    host = settings.get('host')
    user = settings.get('usr')
    password = settings.get('password')
    table = 'biasfairness.aeq_metadata'
    columns = [("name","TEXT"), ("extention","TEXT") , ("schema","TEXT"),
            ("action","TEXT") , ("creator","TEXT"), ("machine","TEXT"),
            ("ip","TEXT"), ("creation_date","TEXT"), ("size","TEXT"),
            ("location","TEXT"), ("status","TEXT"), ("param_bucket","TEXT")]

    def requires(self):
        return Task_71_biasFairnessRDS(nestimators=self.nestimators, maxdepth=self.maxdepth,
                        criterion=self.criterion,year=self.year,month=self.month,day=self.day)

    def rows(self):
        cwd = os.getcwd()  # directorio actual
        BF_md = BF_metadata()
        BF_md.user = str(getpass.getuser())
        BF_md.machine = str(platform.platform())
        BF_md.ip = execv("curl ipecho.net/plain ; echo", cwd)
        BF_md.creation_date = str(datetime.datetime.now())
        BF_md.param_bucket = str(self.bucket)

        meta = BF_md.info()  # extraer información de la clase
        yield (meta)


class Task_80_Predict(luigi.Task):
    '''
    Hace predicciones para la fecha introducida
    '''
    # ==============================
    # parametros:
    # ==============================
    bucket = luigi.Parameter(default="prueba-nyc311")
    nestimators =luigi.Parameter(default=15)
    maxdepth= luigi.Parameter(default=20)
    criterion=luigi.Parameter(default='gini')
    year = luigi.Parameter(default=2020)
    month = luigi.Parameter(default=4)
    day = luigi.Parameter(default=15)
    predictDate = luigi.Parameter()

    # ==============================
    def requires(self):
        return [Task_61_metaModel(nestimators=self.nestimators, maxdepth=self.maxdepth,
                        criterion=self.criterion,year=self.year,month=self.month,day=self.day),
                Task_72_metabiasFairness(nestimators=self.nestimators, maxdepth=self.maxdepth,
                        criterion=self.criterion,year=self.year,month=self.month,day=self.day)
               ]

    def output(self):
        y,m,d=self.predictDate.split("/")
        output_path = f"s3://{self.bucket}/predictions/{y}_{m}_{d}.parquet"
        return luigi.contrib.s3.S3Target(path=output_path)

    def run(self):
        import functionsV1 as f1
        import io
        import numpy as np
        from sklearn.model_selection import TimeSeriesSplit
        from sklearn.ensemble import RandomForestClassifier
        import datetime

        #lectura de datos
        date= datetime.datetime.strptime(self.predictDate, '%Y/%m/%d')
        df=f1.create_prediction_table(date)
        #print(df)

        # Autenticación en S3
        ses = boto3.session.Session(profile_name='luigi_dpa', region_name='us-west-2')
        s3_resource = ses.resource('s3')

        key = f"ml/modelos/{self.criterion}_depth_{self.maxdepth}_estimatros{self.nestimators}_{self.year}_{self.month}_{self.day}.pickle"
        obj = s3_resource.Object(bucket_name=self.bucket, key=key) # objeto
        pkl = obj.get()['Body'].read()
        model=pickle.loads(pkl)
        print(u'\u2B50'*1)
        print("pickle load correctly")
        X = df.values
        preds=model.predict(X)
        print(u'\u2B50'*1)
        boroughs=["bronx","brooklyn","manhattan","queens","staten island","undefined"]
        model_name=f"{self.criterion}_depth_{self.maxdepth}_estimatros{self.nestimators}_{self.year}_{self.month}_{self.day}.pickle"
        print(preds)
        d = {'borough': boroughs,'prediction': preds, 'pred_date':np.repeat(date,6),'model_name':np.repeat(model_name,6)}
        df = pd.DataFrame(data=d)
        print(df)
        df.to_parquet(self.output().path, engine='auto', compression='snappy')

class Task_81_metaPredict(CopyToTable):
    '''
    Guardar los metadatos de las predicciones
    '''
    # ==============================
    # parametros:
    # ==============================
    bucket = luigi.Parameter(default="prueba-nyc311")
    nestimators =luigi.Parameter(default=15)
    maxdepth= luigi.Parameter(default=20)
    criterion=luigi.Parameter(default='gini')
    year = luigi.Parameter(default=2020)
    month = luigi.Parameter(default=4)
    day = luigi.Parameter(default=15)
    predictDate = luigi.Parameter()

    # ==============================
    database = 'nyc311_metadata'
    host = settings.get('host')
    user = settings.get('usr')
    password = settings.get('password')
    table = 'prediction.ejecucion'
    columns = [("pred_date","TEXT"), ("prediction","TEXT") , ("borough","TEXT"),
            ("model_name","TEXT"), ("model_type","TEXT") , ("schema","TEXT"),
            ("action","TEXT") , ("creator","TEXT"), ("machine","TEXT"),
            ("ip","TEXT"), ("date","TEXT"), ("location","TEXT"),
            ("status","TEXT")]

    def requires(self):
        return Task_80_Predict(nestimators=self.nestimators, maxdepth=self.maxdepth,
                    criterion=self.criterion,year=self.year,month=self.month,day=self.day,predictDate=self.predictDate)

    def rows(self):
        import os
        import pandas as pd

    # ==============================
    # se instancia la clase raw_metadata()
        # Autenticación en S3
        ses = boto3.session.Session(profile_name='luigi_dpa', region_name='us-west-2')
        #buffer=io.BytesIO()
        s3_resource = ses.resource('s3')
        obj = s3_resource.Bucket(name=self.bucket)

        #lectura de datos
        y,m,d=self.predictDate.split("/")
        key = f"predictions/{y}_{m}_{d}.parquet"
        parquet_object = s3_resource.Object(bucket_name=self.bucket, key=key) # objeto
        data_parquet_object = io.BytesIO(parquet_object.get()['Body'].read())
        df = pd.read_parquet(data_parquet_object)

        cwd = os.getcwd()  # directorio actual
        model_meta = predict_metadata()
        print(df)
        for index, row in df.iterrows():
            model_meta.pred_date = row['pred_date']
            model_meta.prediction = row['prediction']
            model_meta.borough = row['borough']
            model_meta.model_name = row['model_name']
            model_meta.creator = str(getpass.getuser())
            model_meta.machine = str(platform.platform())
            model_meta.ip = execv("curl ipecho.net/plain ; echo", cwd)
            model_meta.date = str(datetime.datetime.now())
            model_meta.location = f"s3://{self.bucket}/ml/modelos/depth{self.maxdepth}_{self.criterion}_estimatros{self.nestimators}.pickle"

            ubicacion_completa = model_meta.location
            meta = model_meta.info()  # extrae info de la clas
            yield (meta)
