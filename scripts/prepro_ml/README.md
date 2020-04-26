
## Tabla de datos propuesta

| Variable      |  Descripción |
|---------------|---|
| created_day         |  día-mes-año |
| complain_type | tipo de queja = noise  |
| number_cases  | número de casos registrados en el día  |
| mean_month    | promedio de casos en el mes (historico)  |
| **n_c  >  mean_month**| Variable indicadora fecha más casos que el promedio|
<<<<<<< HEAD
|day | dia del mes (1-31)|-
| week_day| día de la semana (1-7) One Hot Encoding|
| month | indicador del mes (1-12) One Hot Encoding|
| year| año de la fecha |
| number_week [ x ] | número de semana (1-52)|
=======
| created_date_day| día de la semana (1-7) One Hot Encoding|
| created_date_month | indicador del mes (1-12) One Hot Encoding|
| created_date_year| año de la fecha |
| number_week | número de semana (1-52)|
>>>>>>> 4c1096173b0cffc7c4f27495879d71ae03641259
| number_cases_week_ago| número de casos del día de la semana pasada|
| mean_cases_3_days_ago| promedio de casos 3 días anteriores|
| mean_cases_3_day/week_ago| promedio de 3 días del mismo día de la semana |
| holiday| indicador de holiday|
| day_before_greater_than_mean_month| indicadora de si el número de casos del día anterior supera la media del mes|
| day/week_before_greater_than_mean_month| indicadora de si el número de casos del día de la semana pasada supera la media del mes|
|numeber_cases_2days_ago| número de casos dos días antes|
|numeber_cases_3days_ago| número de casos tres días antes|
|numeber_cases_4days_ago| número de casos cuatro días antes|
|numeber_cases_5days_ago| número de casos cinco días antes|
|...  | ... |



**Propuestas:**
+ Aumentar las variables cambiando el número de días anteriores: {1,2,3,4,5,6,7,,8,9,10,...}
+ Segmentar por zonas geográficas. La siguiente [liga](https://www.census.gov/en.html "censo usa") contien información por explorar
  + Buscar información de número de pobladores por zona
  + Información del tipo de residencias existentes (número de casas/departamentos/edificios)
  + Ingreso promedio por zona
  + Etnias
