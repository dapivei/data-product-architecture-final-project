#=====================================
# Validación de predicciones
#=====================================
import marbles.core
import pandas as pd

import os
import io
import boto3
import botocore
import getpass
import warnings

# load data from predictions
file = './2021_12_24.parquet'

#df = pd.read_parquet(file)

df = pd.read_parquet(file)

df

class PredictionsTestCase(marbles.core.TestCase):
    DAY=1
    MONTH=1
    YEAR=1
    BUCKET="prueba-nyc311"

    def setUp(self):
        PredictionsTestCase.DAY = os.environ.get('P_DAY', PredictionsTestCase.DAY)
        PredictionsTestCase.MONTH = os.environ.get('P_MONTH', PredictionsTestCase.MONTH)
        PredictionsTestCase.YEAR = os.environ.get('P_YEAR', PredictionsTestCase.YEAR)
        PredictionsTestCase.BUCKET = os.environ.get('P_BUCKET', PredictionsTestCase.BUCKET)

        ses = boto3.session.Session(profile_name='luigi_dpa', region_name='us-west-2')
        buffer=io.BytesIO()
        s3_resource = ses.resource('s3')
        obj = s3_resource.Bucket(name=PredictionsTestCase.BUCKET)

        #lectura de datos
        key = f"predictions/{PredictionsTestCase.YEAR}/{PredictionsTestCase.MONTH}/{PredictionsTestCase.DAY}.parquet"
        parquet_object = s3_resource.Object(bucket_name=PredictionsTestCase.BUCKET, key=key) # objeto
        data_parquet_object = io.BytesIO(parquet_object.get()['Body'].read())#.decode() # info del objeto
        df = pd.read_parquet(data_parquet_object)
        self.df = df

    def tearDown(self):
        delattr(self, 'df')

    def test_binary_predictions(self):
        '''

        Función para evaluar si las predicciones arrojadas son o cero o uno

        '''
        value1 = 0
        value2 = 1
        for i in range(len(df)):
            self.assertTrue((self.df['prediction'][i]==value1) or (self.df['prediction'][i]==value2), note='Las predicciones arrojadas por el modelo tendrían que ser o 0 o 1, cualquier otro tipo de predicción es un error que debe ser subsanado a la brevedad posible.')


    def test_unique_values_borough(self):

        '''

        Función para evaluar que sólo hay cinco valores únicos o menos en la columna de borough

        '''
        unique_boroughs = 6
        self.assertTrue((len(self.df['borough'].unique())<=unique_boroughs) , note='Las predicciones arrojadas por el modelo tendrían que ser para únicamente cinco distritos, si salen más de cinco distritos únicos, habría que revisar el modelo.')

if __name__ == '__main__':
    with warnings.catch_warnings():
        warnings.simplefilter('ignore')
        marbles.core.main(argv=['first-arg-is-ignored'], exit=False)
