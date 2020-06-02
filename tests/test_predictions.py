
#=====================================
# Validación de predicciones
#=====================================

import marbles.core
import pandas as pd


# load data from path_cleaned = 's3://prueba-nyc311/cleaned'
file = './2021_12_24.parquet'

#df = pd.read_parquet(file)

df = pd.read_parquet(file)

df

class PredictionsTestCase(marbles.core.TestCase):

    def setUp(self):
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
        self.assertTrue((len(self.df['borough'].unique())<=unique_boroughs) , note='Las predicciones arrojadas por el modelo tendrían que ser o 0 o 1, cualquier otro tipo de predicción es un error que debe ser subsanado a la brevedad posible.')

if __name__ == '__main__':
    marbles.core.main(argv=['first-arg-is-ignored'], exit=False)
