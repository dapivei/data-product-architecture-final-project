#=====================================
# Unittest for feature engineering
#=====================================

import marbles.core
import pandas as pd
import datetime

# load data from path_ml = s3://prueba-nyc311/ml'
file = '/Users/diegovillalizarraga/Maestria Ciencia de Datos ITAM/2do Semestre/Metodos de gran escala/Repositorio/data-product-architecture-final-project/tests/ml.parquet'
df = pd.read_parquet(file)

# Para las entrega ponemos algun valor de una columna que actualmente es 1 en 0 para hacer que el test falle
#df['y_2010'].iloc[2] = 0
#df['m_1'].iloc[2] = 5

class YearTestCase(marbles.core.TestCase):

    def setUp(self):
        self.df = df

    def tearDown(self):
        delattr(self, 'df')

    def test_created_date_year_vs_onehot(self):
        '''

        Función para evaluar si es consistente el año del created_date (year) vs el
        one hot y_{year} == 1

        '''

        for row in range(0, self.df.shape[0]):
            _year = self.df['created_date'].iloc[row].year
            _valid = self.df[f'y_{_year}'].iloc[row]
            self.assertEqual(_valid, 1,
            msg = f'La fila {row} para la columna y_{_year} tiene un 0 cuando debería de tener 1.',
            note = 'Si sacamos el año de la fecha created date (year) debería de estar como True (1) la bandera en la columna de año correspondiente con formato onehot para garantizar consistencia.')


    def test_created_date_month_vs_onehot(self):
        '''
        Función para evaluar si es consistente el mes del created_date (month) vs el
        one hot m_{month} == 1
        '''

        for row in range(0, self.df.shape[0]):
            _month = self.df['created_date'].iloc[row].month
            _valid = self.df[f'm_{_month}'].iloc[row]
            self.assertEqual(_valid, 1,
            msg = f'La fila {row} para la columna m_{_month} tiene un 0 cuando debería de tener 1.',
            note = 'Si sacamos el mes de la fecha created date (year) debería de estar como True (1) la bandera en la columna de mes correspondiente con formato onehot para garantizar consistencia.')


if __name__ == '__main__':
    marbles.core.main()
