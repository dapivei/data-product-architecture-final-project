import pandas as pd
from pandas.testing import assert_frame_equal
import sys

# path a leer
df_o = pd.read_parquet('./tests/ml.parquet')


class NumberCases():

    def prueba_casos_dia(self):
        # Para calcular segun el distrito (descomentar las siguientes lineas)
        # distrito = ['dist1','dist2','dist3','dist4','dist5','dist6']
        # for j in distrito:
        #   df = df_o[df_o[i] == 1]
        self.df = df_o  # comentar esta linea cuando se tengan los distritos
        largo_base = len(self.df['counts'])-1
        history_days = 10
        for i in range(1, history_days):
            var_name = "number_cases_" + str(i) + "_days_ago"
            a = self.df.loc[0:largo_base-1, ['counts']].reset_index(drop=True)
            b = self.df.loc[i:largo_base, [var_name]].reset_index(drop=True)
            b.columns = ['counts']
            try:
                assert_frame_equal(a, b, check_column_type=False,
                                   check_dtype=False,
                                   check_names=False)
                print("Sin error en la variable: " + var_name)
            except:
                # En que variable ocurrio el error
                print("El error ocurre en la variable:  " + var_name)
                # Error de la prueba
                print(sys.exc_info()[1])


if __name__ == '__main__':
    NumberCases.prueba_casos_dia(NumberCases)
