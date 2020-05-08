#=====================================
# Unittest for cleaned data
#=====================================

import marbles.core
import pandas as pd
import datetime
from datetime import date


# load data from path_cleaned = 's3://prueba-nyc311/cleaned'
file = '/Users/diegovillalizarraga/Maestria Ciencia de Datos ITAM/2do Semestre/Metodos de gran escala/Repositorio/data-product-architecture-final-project/tests/clean_2015_4_14.parquet'
df = pd.read_parquet(file)


class DateTestCase(marbles.core.TestCase):

    def setUp(self):
        self.df = df

    def tearDown(self):
        delattr(self, 'df')

    def test_for_closed_date_greater_than_created_date(self):
        '''

        Función para evaluar si la closed_date es mayor a created_date

        '''

        _filtro = self.df['closed_date'] < self.df['created_date']
        msg = f'Se detectaron {self.df[_filtro].shape[0]} casos en los que la closed_date sucede antes que la created_date.'
        note = 'Es imposible que un issue se cierre antes de ser abierto, actualmente existen casos en los que esta sucediendo esto.'

        if df['closed_date'] is not None:
            _valid = self.df['closed_date'] >= self.df['created_date']
            self.assertTrue(all(_valid), msg=msg, note=note)



    def test_for_years_out_of_range(self):
        '''

        Evaluando que los valores de created_date y closed_date están entre 2010 y
        la fecha actual.

        '''

        minimum = datetime.date(2010,1,1)
        today = date.today()
        self.assertGreaterEqual(min(self.df['created_date']), minimum, note='La fuente de donde se obtuvieron los datos se especifica que son registros del 2010 en adelante por lo que no tendria sentido tener registros con created_date previos al 2010.')
        self.assertLessEqual(max(self.df['created_date']), today, note='No tendria sentido que hubiera una created_date posterior a la fecha del dia de hoy.')
        self.assertGreaterEqual(min(self.df['closed_date']), minimum, note='La fuente de donde se obtuvieron los datos se especifica que son registros del 2010 en adelante por lo que no tendria sentido tener registros con closed_date previos al 2010.')
        self.assertLessEqual(max(self.df['closed_date']), today, note='No tendria sentido que hubiera una close_date posterior a la fecha del dia de hoy.')


if __name__ == '__main__':
    marbles.core.main()
