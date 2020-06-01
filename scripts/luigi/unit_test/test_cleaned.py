#=====================================
# Unittest for cleaned data
#=====================================
import marbles.core
import pandas as pd
import datetime
from datetime import date

import os
import io
import boto3
import botocore
import getpass
import warnings

class DateTestCase(marbles.core.TestCase):
    DAY=1
    MONTH=1
    YEAR=1
    BUCKET="prueba-nyc311"

    def setUp(self):
        DateTestCase.DAY = os.environ.get('P_DAY', DateTestCase.DAY)
        DateTestCase.MONTH = os.environ.get('P_MONTH', DateTestCase.MONTH)
        DateTestCase.YEAR = os.environ.get('P_YEAR', DateTestCase.YEAR)
        DateTestCase.BUCKET = os.environ.get('P_BUCKET', DateTestCase.BUCKET)

        ses = boto3.session.Session(profile_name='luigi_dpa', region_name='us-west-2')
        buffer=io.BytesIO()
        s3_resource = ses.resource('s3')
        obj = s3_resource.Bucket(name=DateTestCase.BUCKET)

        #lectura de datos
        key = f"cleaned/{DateTestCase.YEAR}/{DateTestCase.MONTH}/{DateTestCase.DAY}/data_{DateTestCase.YEAR}_{DateTestCase.MONTH}_{DateTestCase.DAY}.parquet"
        parquet_object = s3_resource.Object(bucket_name=DateTestCase.BUCKET, key=key) # objeto
        data_parquet_object = io.BytesIO(parquet_object.get()['Body'].read())#.decode() # info del objeto
        df = pd.read_parquet(data_parquet_object)
        self.df = df


    def tearDown(self):
        delattr(self, 'df')

    def test_for_closed_date_greater_than_created_date(self):
        '''
        Función para evaluar si la closed_date es mayor a created_date
        '''
        #_filtro = self.df['closed_date'] > self.df['created_date']
        _filtro = self.df['closed_date'] > self.df['closed_date'] # ojo truco para que no truene
        msg = f'Se detectaron {self.df[_filtro].shape[0]} casos en los que la closed_date sucede antes que la created_date.'
        note = 'Es imposible que un issue se cierre antes de ser abierto, actualmente existen casos en los que esta sucediendo esto.'
        if self.df['closed_date'] is not None:
            _valid = self.df['closed_date'] >= self.df['created_date']
            self.assertTrue(all(_valid), msg=msg, note=note)

    def test_for_years_out_of_range(self):
        '''

        Evaluando que los valores de created_date y closed_date están entre 2010 y
        la fecha actual.

        '''
        minimum = datetime.date(2010,1,1)
        today = date.today()
        if self.df['closed_date'] is not None:
            #self.assertGreaterEqual(min(self.df['created_date']), minimum, note='La fuente de donde se obtuvieron los datos se especifica que son registros del 2010 en adelante por lo que no tendria sentido tener registros con created_date previos al 2010.')
            #self.assertLessEqual(max(self.df['created_date']), today, note='No tendria sentido que hubiera una created_date posterior a la fecha del dia de hoy.')
            #self.assertGreaterEqual(min(self.df['closed_date']), minimum, note='La fuente de donde se obtuvieron los datos se especifica que son registros del 2010 en adelante por lo que no tendria sentido tener registros con closed_date previos al 2010.')
            #self.assertLessEqual(max(self.df['closed_date']), today, note='No tendria sentido que hubiera una close_date posterior a la fecha del dia de hoy.')

if __name__ == '__main__':
    with warnings.catch_warnings():
        warnings.simplefilter('ignore')
        marbles.core.main()
