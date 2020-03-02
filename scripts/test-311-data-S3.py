import luigi
import luigi.contrib.s3
import boto3
import os
import pandas as pd
import json
from sodapy import Socrata

# path para guardar los datos
output_path='/Users/c1587s/Documents/GitHub/data-product-architecture/scripts/data-311/test.json'

class downloadDataS3(luigi.Task):
    task_name = "demo"

    bucket = luigi.Parameter()
    root_path = luigi.Parameter()
    etl_path = luigi.Parameter()
    year = luigi.Parameter()
    month = luigi.Parameter()

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
        client.timeout = 50
        results = client.get("erm2-nwe9", limit=2)

        with self.output().open('w') as json_file:
            json.dump(results, json_file)

    def output(self):
        '''
        Descarga los datos en path sleccionado
        '''
        output_path = "s3://{}/{}/{}/{}/YEAR={}/MONTH={}/test.csv".\
        format(self.bucket,
        self.root_path,
        self.etl_path,
        self.task_name,
        self.year,
        str(self.month))

        return luigi.contrib.s3.S3Target(path=output_path)
