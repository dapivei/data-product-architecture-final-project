#!/usr/bin/env python
# -*- coding: utf-8 -*-

import json
import luigi
import os
import pandas as pd
import functions   # modulo propio
import psycopg2 as ps

from datetime import date
from dynaconf import settings
from pyspark import SparkContext
from pyspark.sql import SQLContext
from sodapy import Socrata

import subprocess as sub

# Definir los paths donde se guardan los datos
path_raw = './raw'
path_preproc = './preprocess'

class downloadRawJSONData(luigi.Task):
    '''
    Descarga los datos de la API de 311 NYC en formato JSON en carpetas por
    fecha con frecuencia diaria.
    '''
    # parametros:
    year = luigi.Parameter()
    month = luigi.Parameter()
    day = luigi.Parameter()

    def output(self):
        output_path = f"{path_raw}/{self.year}/{self.month}/{self.day}/data_{self.year}_{self.month}_{self.day}.json"
        return luigi.local_target.LocalTarget(path=output_path)

    def run(self):
        # Autenticación del cliente:
        client = Socrata(settings.get('dburl'),
                         settings.get('apptoken'),
                         username=settings.get('user'),
                         password=settings.get('pass'))

        # los resultados son retornados como un archivo JSON desde la API /
        # convertida a una lista de Python usando sodapy
        client.timeout = 1000
        limit = 1000000000

        # crear carpeta raw
        if not os.path.exists(f'{path_raw}'):
            os.mkdir(f'{path_raw}')
        else:
            None

        # crear carpeta year
        if not os.path.exists(f'{path_raw}/{self.year}'):
            os.mkdir(f'{path_raw}/{self.year}')
        else:
            None

        # crear carpeta year/month
        if not os.path.exists(f'{path_raw}/{self.year}/{self.month}'):
            os.mkdir(f'{path_raw}/{self.year}/{self.month}')
        else:
            None

        # crear carpeta  year/month/day
        if not os.path.exists(f'{path_raw}/{self.year}/{self.month}/{self.day}'):
            os.mkdir(f'{path_raw}/{self.year}/{self.month}/{self.day}')
        else:
            None

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
    year = luigi.Parameter()
    month = luigi.Parameter()
    day = luigi.Parameter()

    def requires(self):
        return downloadRawJSONData(year=self.year, month=self.month, day=self.day)

    def output(self):
        output_path = f"{path_preproc}/{self.year}/{self.month}/{self.day}/data_{self.year}_{self.month}_{self.day}.parquet"
        return luigi.local_target.LocalTarget(path=output_path)

    def run(self):
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
        # lineas para correrlo sin y con requirements de downloadRawJSONData
        # df = pd.read_json(f"{path_raw}/{self.year}/{self.month}/{self.day}/data_{self.year}_{self.month}_{self.day}.json")
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
    year = luigi.Parameter()
    month = luigi.Parameter()
    day = luigi.Parameter()

    def requires(self):
        #return downloadRawJSONData(year=self.year, month=self.month, day=self.day)
        return metaExtract(year=self.year, month=self.month, day=self.day)
    def output(self):
        output_path = f"{path_preproc}/{self.year}/{self.month}/{self.day}/"
        return luigi.local_target.LocalTarget(path=output_path)

    def run(self):
        # generar los metadatos de Download

        metaExtract(year=self.year, month=self.month, day=self.day)
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
        df = sqlContext.read.json(f"{path_raw}/{self.year}/{self.month}/{self.day}/data_{self.year}_{self.month}_{self.day}.json")
        #df = sqlContext.read.json(cwd)
        #df = sqlContext.read.json(self.input().path)

        # guardar como parquet
        self.output().makedirs()
        df.write.parquet(self.output().path, mode="overwrite")


class metaExtract(luigi.Task):
    '''
    Generar metadatos de la extracción de datos de la API
    '''
    # parámetros
    year = luigi.Parameter()
    month = luigi.Parameter()
    day = luigi.Parameter()

    def requires(self):
        return downloadRawJSONData(year=self.year, month=self.month, day=self.day)

    def output(self):
        output_path = f"{path_raw}/{self.year}/{self.month}/{self.day}/metaData_{self.year}_{self.month}_{self.day}.csv"
        return luigi.local_target.LocalTarget(path=output_path)

    def run(self):
        cwd = os.getcwd()  # path actual
        file_path = self.input().path
        metadat= functions.get_extract_metadata(file_path,cwd)

        conn=ps.connect(host=settings.get('host'),
                        port=settings.get('port'),
                        database=settings.get('database'),
                        user=settings.get('usr'),
                        password=settings.get('password'))
        cur = conn.cursor()
        sql="INSERT INTO raw.etl_execution VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);"
        cur.execute(sql,metadat)
        conn.commit()
        cur.close()
        conn.close()

        #file_name = functions.execv(cmd_name, cwd)
        '''
        cmd_name = "echo %s | awk -F \"/\" \'{print $NF}\'" % (file_path)
        # obterner solo el nombre del archivo

        # crea df vacío usando pandas
        columns = ['name', 'extention', 'schema', 'action', 'creator', 'machine', 'ip', 'creation_date', 'size', 'location','entries', 'variables', 'script', 'log_script', 'status']
        df = pd.DataFrame(columns=columns)
        # defnir los comandos a utilizar para llenar las celdas
        name_cmd = "echo  %s | awk -F\".\" \'{print $1}\'" % (file_name)
        ext_cmd = "ls -lad %s | awk -F\".\" \'{print $NF}\' " % (file_path)
        cre_cmd = "ls -lad %s | awk \'{print $3}\'" % (file_path)
        mch_cmd = "  uname -a"
        ip_cmd = "curl ipecho.net/plain ; echo"
        cdt_cmd = "ls -lad %s | awk \'{print $6\"-\"$7\"-\"$8}'" % (file_path)
        siz_cmd = "ls -lad -h %s | awk \'{print $5}\'" % (file_path)
        ent_cmd = "jq length %s" % (file_path)

        # llena el df
        df.at[0, 'name'] = functions.execv(name_cmd, cwd)
        df.at[0, 'extention'] = functions.execv(ext_cmd, cwd)
        df.at[0, 'schema'] = 'raw'
        df.at[0, 'action'] = 'download'
        df.at[0, 'creator'] = functions.execv(cre_cmd, cwd)
        df.at[0, 'machine'] = functions.execv(mch_cmd, cwd)
        df.at[0, 'ip'] = functions.execv(ip_cmd, cwd)
        df.at[0, 'creation_date'] = functions.execv(cdt_cmd, cwd)
        df.at[0, 'size'] = functions.execv(siz_cmd, cwd)
        df.at[0, 'location'] = file_path
        df.at[0, 'entries'] = functions.execv(ent_cmd, cwd)
        df.at[0, 'variables'] = None
        df.at[0, 'script'] = None
        df.at[0, 'log_script'] = None
        df.at[0, 'status'] = None
        '''
        # escribir csv para guardar la info
        self.output().makedirs()
        #df.to_csv(self.output().path, mode="w+", index=False)


class metaPreproc(luigi.Task):
    '''
    Generar metadatos del preprocesamiento de datos, donde se convierten de JSON
    a parquet.
    '''
    # parámetros
    year = luigi.Parameter()
    month = luigi.Parameter()
    day = luigi.Parameter()

    def requires(self):
        return preprocParquetSpark(year=self.year, month=self.month, day=self.day)
        #return preprocParquetPandas(year=self.year, month=self.month, day=self.day)

    def output(self):
        output_path = f"{path_preproc}/{self.year}/{self.month}/{self.day}/metaData_{self.year}_{self.month}_{self.day}.csv"
        return luigi.local_target.LocalTarget(path=output_path)

    def run(self):
        cwd = os.getcwd()  # path actual
        file_path = self.input().path
        # encontrar todos los archivos formato parquet
        ls_parquet_files = functions.execv("ls *.parquet", file_path)
        names_file=ls_parquet_files.split('\n')
        cmd_name = "echo %s | awk -F \"/\" \'{print $NF}\'" % (file_path)
        # obterner solo el nombre del archivo
        file_name = functions.execv(cmd_name, cwd)
        # crea df vacío usando pandas
        columns = ['name', 'extention', 'schema', 'action','creator', 'machine', 'ip', 'creation_date','size', 'location','entries', 'variables', 'script', 'log_script', 'status']
        df = pd.DataFrame(columns=columns)
        # defnir los comandos a utilizar para llenar las celdas
        count = 0

        for file in names_file:
            # introducir nombre
            cmd_name = "echo %s | awk -F \"/\" \'{print $NF}\'" % (file)
            #df.at[count, 'name' ] = functions.execv(cmd_name, cwd)
            cmd_name=functions.execv(cmd_name, cwd)
            # introducir extension
            ext_cmd = "ls -lad %s | awk -F\".\" \'{print $NF}\' " % (file)
            df.at[count, 'extention'] = functions.execv(ext_cmd, cwd)
            # esquema y acción
            df.at[count, 'schema'] = 'preprocessing'
            df.at[count, 'action'] = 'transform json to parquet'
            # otras características de la creación
            cre_cmd = "ls -lad %s | awk \'{print $3}\'" % (file)
            df.at[count, 'creator'] = functions.execv(cre_cmd, cwd)
            mch_cmd = "uname -a"
            df.at[count, 'machine'] = functions.execv(mch_cmd, cwd)
            ip_cmd = "curl ipecho.net/plain ; echo"
            df.at[count, 'ip'] = functions.execv(ip_cmd, cwd)
            cdt_cmd = "ls -lad %s | awk \'{print $6\"-\"$7\"-\"$8}\'" % (file)
            df.at[count, 'creation_date'] = functions.execv(cdt_cmd, cwd)
            siz_cmd = "ls -lad -h %s | awk \'{print $5}\'" % (file)
            df.at[count, 'size'] = functions.execv(siz_cmd, cwd)
            df.at[count, 'location'] = file_path
            count += 1

        vars=(cmd_name,cdt_cmd)
        conn=ps.connect(host=settings.get('host'),
                        port=settings.get('port'),
                        database=settings.get('database'),
                        user=settings.get('usr'),
                        password=settings.get('password'))
        cur = conn.cursor()
        sql="INSERT INTO raw.etl_execution (name,action) VALUES (%s,%s);"
        #sql="INSERT INTO raw.etl_execution ('name', 'extention', 'schema', 'action','creator',"
        #sql=sql+ " 'machine', 'ip', 'creation_date','size', 'location','entries', 'variables', 'script', 'log_script', 'status') VALUE (1,1,1,1,1,1,1,1,1,1,1,1,1,1,1)"

        cur.execute(sql,vars)

        conn.commit()
        cur.close()
        conn.close()

        # escribir csv para guardar la info
        self.output().makedirs()
        df.to_csv(self.output().path, mode="w+", index=False)

class ELTprocess(luigi.Task):
        '''
        Corre el proceso de ELT generando metadatos.
        '''
        # parámetros
        year = luigi.Parameter()
        month = luigi.Parameter()
        day = luigi.Parameter()

        def requires(self):
            return metaPreproc(year=self.year, month=self.month, day=self.day),
