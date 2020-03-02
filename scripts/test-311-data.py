#!/usr/bin/env python
# -*- coding: utf-8 -*-

import luigi
import json
from sodapy import Socrata

# path para guardar los datos
outputpath='/PATH.../test.json'

class downloadData(luigi.Task):

    def run(self):
        '''
        Consulta de los datos en la api de 311
        '''
        # Autenticación del cliente:
        client = Socrata("data.cityofnewyork.us",
                        "N2WpW61JnP5RoT5mrYGUaSUg9",
                        username="****",
                        password="****")

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
