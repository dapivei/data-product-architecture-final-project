#!/usr/bin/env python
# -*- coding: utf-8 -*-

import luigi
import json
from sodapy import Socrata
from dynaconf import settings

# path para guardar los datos
outputpath='/PATH.../test.json'

class downloadData(luigi.Task):

    def run(self):
        '''
        Consulta de los datos en la api de 311
        '''
        # Autenticación del cliente:
        client = Socrata(settings.get('dburl'),
                settings.get('apptoken'),
                username=settings.get('user'),
                password=settings.get('pass'))

        # los resultados son retornados como un archivo JSON desde la API /
        # convertida a una lista de Python usando sodapy
        client.timeout = 50
        results = client.get("erm2-nwe9", limit=1)

        with self.output().open('w') as json_file:
            json.dump(results, json_file)

    def output(self):
        '''
        Descarga los datos en `path`
        `path`: ruta donde va a guardar los datos descargados.
        '''
        return luigi.local_target.LocalTarget(outputpath)
