#!/usr/bin/env python
# -*- coding: utf-8 -*-

import luigi
import luigi.contrib.s3
import boto3
import os
import json
from sodapy import Socrata
from dynaconf import settings

# path para guardar los datos

class downloadDataS3(luigi.Task):
    task_name = "demo"

    bucket = luigi.Parameter()
    root_path = luigi.Parameter()
    etl_path = luigi.Parameter()
    data_str = luigi.Parameter()
    schema = luigi.Parameter()

    def run(self):
        '''
        Consulta de los datos en la api de 311
        '''
        # Autenticación en S3
        ses = boto3.session.Session(profile_name='luigi_dpa', region_name='us-west-2')
        s3_resource = ses.resource('s3')

        obj = s3_resource.Bucket(self.bucket)
        print(ses)
        # Autenticación del cliente:
       client = Socrata("data.cityofnewyork.us",
                        "N2WpW61JnP5RoT5mrYGUaSUg9",
                        username="villa.lizarraga@gmail.com",
                        password="Itam1234567890@")

        # los resultados son retornados como un archivo JSON desde la API /
        # convertida a una lista de Python usando sodapy
        client.timeout =1000
        results = client.get("erm2-nwe9", limit=100)

        with self.output().open('w') as json_file:
            json.dump(results, json_file)

    def output(self):
        '''
        Descarga los datos en path sleccionado
        '''
        output_path = "s3://{}/{}/{}/{}/{}/{}/DataTest.json".\
        format(self.bucket,
        self.root_path,
        self.etl_path,
        self.task_name,
        self.data_str,
        str(self.schema))

        return luigi.contrib.s3.S3Target(path=output_path)
