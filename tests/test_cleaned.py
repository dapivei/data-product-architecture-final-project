#=====================================
# Unittest for cleaned data
#=====================================

import unittest
import pandas as pd
import datetime
from datetime import date

# load data from path_cleaned = 's3://prueba-nyc311/cleaned'
df = pd.read_json("/Users/danielapintoveizaga/GitHub/data-product-architecture-final-project/scripts/luigi/raw/2013/10/6/data_2013_10_6.json")

#df = pd.read_parquet("/Users/danielapintoveizaga/Downloads/data_2011_10_10.parquet") # corregir path

class DateTestCase(unittest.TestCase): #inheriting from unittest.TestCase

#    def setUp(self):        # hay que corregir esta parte no está funcionado
#        self.df = df        # load data from path_cleaned = 's3://prueba-nyc311/cleaned'

#    def tearDown(self):
#        delattr(self, 'df') # taking the data of my test case
                            # \to avoid pollution

    def test_for_closed_date_greater_than_created_date(self):
        '''

        Función para evaluar si la closed_date es igual o mayor a created_date

        '''

        valid = df['closed_date'] >= df['created_date']
        self.assertTrue(all(valid), note = "Si la prueba falla, puede deberse a que la columna closed_date tiene valor nulo.")


    def test_for_years_out_of_range(self):

        '''

        Evaluando que los valores de created_date y closed_date están entre 2010 y
        la fecha actual.

        '''

        minimun = datetime.date(2010,1,1)
        today = date.today()
        self.assertGreaterEqual(min(df['created_date']), minimun, note= "Si la prueba falla, puede deberse a que los datos fueron actualizados y se incluyeron registros que previamente no existían.")
        self.assertLessEqual(max(df['created_date']), today)
        self.assertGreaterEqual(min(df['closed_date']), minimun, note= "Si la prueba falla, puede deberse a que los datos fueron actualizados y se incluyeron registros que previamente no existían.")
        self.assertLessEqual(max(df['closed_date']), today)


if __name__ == '__main__':
    unittest.main()


#import marbles.core

#class LowerTestCase(marbles.core.TestCase):

#    def test_lower_case(self):
#        cols = df.columns
#        for col in cols:
#            try:
#                self.assertEqual(df[col], df[col].str.lower(), note= "En el guardado 'cleaned', para tener datos homegeneos, convertimos todos los valores strings a minúsculas")
#            except:
#                next

        #self.assertEqual(df.upper(), 'FOO', note=" the names should be uppercase because bla bla bla")

    #def test_isupper_w_marbles(self):
    #    self.assertTrue('foo'.isupper())
    #    self.assertFalse('Foo'.isupper())

    #def test_split_w_marbles(self):
    #    s = 'hello world'
    #    self.assertEqual(s.split(), ['hello', 'world'])
#        # check that s.split fails when the separator is not a string
#        with self.assertRaises(TypeError):
#            s.split(2)
