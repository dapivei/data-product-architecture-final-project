#!/usr/bin/env python
# -*- coding: utf-8 -*-
import json
import luigi
import os
import pandas as pd

from datetime import date
from dynaconf import settings
from sodapy import Socrata

# Definir los paths donde se guardan los datos
path_raw = './raw'
path_preproc ='./preprocess'


# path para guardar los datos
class downloadRawJSONData(luigi.Task):
    '''
    Descarga los datos de la API de 311 NYC en formato JSON en carpetas por
    fecha con frecuencia diaria.
    '''
    # parametros:
    year  = luigi.Parameter()
    month = luigi.Parameter()
    day   = luigi.Parameter()

    def output(self):
        # Defining the loop for creating the variables:
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
        l = 1000000000

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
        results = client.get("erm2-nwe9", limit=l, where=f"created_date between '{self.year}-{self.month}-{self.day}T00:00:00.000' and '{self.year}-{self.month}-{self.day}T23:59:59.999'")
        with self.output().open('w') as json_file:
            json.dump(results, json_file)

class preprocPARQUET(luigi.Task):
    '''
    Convertir datos descargados en JSON a formato PARQUET.
    '''
    # parametros
    year  = luigi.Parameter()
    month = luigi.Parameter()
    day   = luigi.Parameter()

    def output(self):
        output_path = f"{path_preproc}/{self.year}/{self.month}/{self.day}/data_{self.year}_{self.month}_{self.day}.parquet"
        return luigi.local_target.LocalTarget(path=output_path)

    #def requires(self:
    #    None

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
        df = pd.read_json(f"{path_raw}/{self.year}/{self.month}/{self.day}/data_{self.year}_{self.month}_{self.day}.json")
        # Solving problems of datatype: "nested column branch had multiple children"
        for col in df.columns:
            weird = (df[[col]].applymap(type) != df[[col]].iloc[0].apply(type)).any(axis=1)
            if len(df[weird]) > 0:
                df[col] = df[col].astype(str)
            if df[col].dtype == list:
                df[col] = df[col].astype(str)

        # guardar como parquet
        self.output().makedirs()
        df.to_parquet(self.output().path, engine='auto', compression='snappy')
