#!/usr/bin/env python
# -*- coding: utf-8 -*-
import json
import luigi
import os

from datetime import date
from dynaconf import settings
from sodapy import Socrata

# path para guardar los datos
#path = '/Users/c1587s/Desktop/db-diego/raw/json'
path = '/Users/scadavidsanchez/Desktop/raw'


class downloadRawJSONData(luigi.Task):
    '''
    Parameters:
     -
    '''
    year = luigi.Parameter()
    month = luigi.Parameter()
    day = luigi.Parameter()

    def output(self):
        today = str(date.today())
        # Defining the loop for creating the variables:
        output_path = f"{path}/{self.year}/{self.month}/{self.day}/data_{self.year}_{self.month}_{self.day}.json"
        return luigi.local_target.LocalTarget(path=output_path)

    def run(self):
        '''
        Descarga los datos de la API de 311 NYC en formato JSON en carpetas por
        fecha con frecuencia diaria desde 2010-01-01.
        '''
        # Autenticación del cliente:
        client = Socrata(settings.get('dburl'),
                         settings.get('apptoken'),
                         username=settings.get('user'),
                         password=settings.get('pass'))

        # los resultados son retornados como un archivo JSON desde la API /
        # convertida a una lista de Python usando sodapy
        client.timeout = 1000
        l = 1000000000
        today = str(date.today())

        if not os.path.exists(f'{path}'):
            os.mkdir(f'{path}')
        else:
            None

        # year path
        if not os.path.exists(f'{path}/{self.year}'):
            os.mkdir(f'{path}/{self.year}')
        else:
            None

        # year/month path
        if not os.path.exists(f'{path}/{self.year}/{self.month}'):
            os.mkdir(f'{path}/{self.year}/{self.month}')
        else:
            None

        # complete daily-path
        if not os.path.exists(f'{path}/{self.year}/{self.month}/{self.day}'):
            os.mkdir(f'{path}/{self.year}/{self.month}/{self.day}')
        else:
            None

        # query
        results = client.get("erm2-nwe9", limit=l, where=f"created_date between '{self.year}-{self.month}-{self.day}T00:00:00.000' and '{self.year}-{self.month}-{self.day}T23:59:59.999'")
        with self.output().open('w') as json_file:
            json.dump(results, json_file)
