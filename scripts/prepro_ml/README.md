
## Datos generados durante el Preprocessing de Machine Learning
***
<div align=justify>

A partir de la variable *created_date (día-mes-año en que se levantó la queja)* se crearon las siguientes variables que serán empleadas en el modelado:


| Variable      |  Descripción |
|---------------|---|
| complain_type | tipo de queja = noise  |
| number_cases  | número de casos registrados en el día  |
| mean_month    | promedio de casos registrados en un mes (histórico)  |
| **number_cases  >  mean_month**| variable indicadora -- día con número de quejas superiores al promedio del mes|
| created_date_day| día del mes en que se levantó la queja (1-31) -- one hot encoding|
| created_date_month | indicador del mes de registro de la queja (1-12) -- one hot encoding|
|created_date_dow| día de la semana en que se levantó la queja (1-7) -- one hot encoder|
| created_date_year| año de registro de la queja -- one hot encoding |
| created_date_woy| número de semana del año en que fue registrada la queja (1-52) -- one hot encoding|
| date_holiday | indicador binario si el día en que se registro la queja fue un feriado o no|
|number_cases_1day_ago| número de casos registrados 1 día antes|
|number_cases_2days_ago| número de casos registrados dos días antes|
|number_cases_3days_ago| número de casos registrados tres días antes|
|number_cases_4days_ago| número de casos registrados cuatro días antes|
|number_cases_5days_ago| número de casos registrados cinco días antes|


El código asociado a la creación de cada una de estas variables, se encuentra en las siguientes documents:

> [NumberCases.py](https://github.com/dapivei/data-product-architecture-final-project/blob/master/scripts/prepro_ml/NumberCases.py)

> [Feature.py](https://github.com/dapivei/data-product-architecture-final-project/blob/master/scripts/prepro_ml/feature.py)

> [holidasyUsa.py](https://github.com/dapivei/data-product-architecture-final-project/blob/master/scripts/prepro_ml/holidaysUsa.py)

> [oneHotEncoders.py](https://github.com/dapivei/data-product-architecture-final-project/blob/master/scripts/prepro_ml/oneHotEncoders.py)

**Propuestas a futuro:**
+ Aumentar las variables cambiando el número de días anteriores: {1,2,3,4,5,6,7,,8,9,10,...}
+ Segmentar por zonas geográficas. La siguiente [liga](https://www.census.gov/en.html "censo usa") contiene información por explorar:
  + Buscar información de número de pobladores por zona
  + Información del tipo de residencias existentes (número de casas/departamentos/edificios)
  + Ingreso promedio por zona
  + Etnias

</div>
