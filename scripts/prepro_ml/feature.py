import pyspark

from pyspark.sql.functions import date_add
from pyspark.sql.functions import weekofyear
from pyspark.sql.functions import date_format
from pyspark.sql.functions import dayofmonth
from pyspark.sql.functions import month
from pyspark.sql.functions import year
from pyspark.sql.functions import col
from pyspark.sql.functions import from_unixtime, unix_timestamp
from pyspark.sql.types import StringType

history_days = 10

#Importamos el dataframe (Esta sera la ruta que ira al schema clean)
df = spark.read.parquet("s3://prueba-nyc311/cleaned/*/*/*/data_*_*_*.parquet")


# Hacemos una copia del dataframe original
df3 = df

# Filtramos los casos de ruido para la agencia NYPD
df3 = df3.filter(col('agency').isin('nypd'))
df3 = df3.filter(col('complaint_type').contains('noise'))
df3 = df3.drop('incident_zip')
df3 = df3.drop('agency')
df3 = df3.drop('agency_name')
df3 = df3.drop('complaint_type')


# Created date (Obtenemos año, mes y dia)
df3 = df3.withColumn('created_date_year', year(df3['created_date']))
df3 = df3.withColumn('created_date_month', month(df3['created_date']))
df3 = df3.withColumn('created_date_day', dayofmonth(df3['created_date']))

# Creamos la variable created_date_dow (numero del dia de la semana de la fecha created_date)
df3 = df3.withColumn('created_date_dow', date_format(df3['created_date'], 'u').alias('created_date_dow'))

# Creamos la variable created_date_woy (numero de la semana del año de la fecha created_date)
df3 = df3.withColumn('created_date_woy', weekofyear(df3['created_date']).alias("created_date_woy"))


# Creamos variable holiday
h = ['2010-01-01', '2010-12-31', '2010-01-18', '2010-02-15', '2010-05-31', '2010-07-04', '2010-07-05', '2010-09-06', '2010-10-11', '2010-11-11', '2010-11-25', '2010-12-25', '2010-12-24', '2011-01-01', '2010-12-31', '2011-01-17', '2011-02-21', '2011-05-30', '2011-07-04', '2011-09-05', '2011-10-10', '2011-11-11', '2011-11-24', '2011-12-25', '2011-12-26', '2012-01-01', '2012-01-02', '2012-01-16', '2012-02-20', '2012-05-28', '2012-07-04', '2012-09-03', '2012-10-08', '2012-11-11', '2012-11-12', '2012-11-22', '2012-12-25', '2013-01-01', '2013-01-21', '2013-02-18', '2013-05-27', '2013-07-04', '2013-09-02', '2013-10-14', '2013-11-11', '2013-11-28', '2013-12-25', '2014-01-01', '2014-01-20', '2014-02-17', '2014-05-26', '2014-07-04', '2014-09-01', '2014-10-13', '2014-11-11', '2014-11-27', '2014-12-25', '2015-01-01', '2015-01-19', '2015-02-16', '2015-05-25', '2015-07-04', '2015-07-03', '2015-09-07', '2015-10-12', '2015-11-11', '2015-11-26', '2015-12-25', '2016-01-01', '2016-01-18', '2016-02-15', '2016-05-30', '2016-07-04', '2016-09-05', '2016-10-10', '2016-11-11', '2016-11-24', '2016-12-25', '2016-12-26', '2017-01-01', '2017-01-02', '2017-01-16', '2017-02-20', '2017-05-29', '2017-07-04', '2017-09-04', '2017-10-09', '2017-11-11', '2017-11-10', '2017-11-23', '2017-12-25', '2018-01-01', '2018-01-15', '2018-02-19', '2018-05-28', '2018-07-04', '2018-09-03', '2018-10-08', '2018-11-11', '2018-11-12', '2018-11-22', '2018-12-25', '2019-01-01', '2019-01-21', '2019-02-18', '2019-05-27', '2019-07-04', '2019-09-02', '2019-10-14', '2019-11-11', '2019-11-28', '2019-12-25', '2020-01-01', '2020-01-20', '2020-02-17', '2020-05-25', '2020-07-04', '2020-07-03', '2020-09-07', '2020-10-12', '2020-11-11', '2020-11-26', '2020-12-25']

h = spark.createDataFrame(h, StringType())
h = h.withColumnRenamed("value", "date_holiday")
h = h.withColumn("date_holiday", from_unixtime(unix_timestamp('date_holiday', 'yyyy-MM-dd')).alias('date_holiday').cast("date"))
h = h.withColumn('holiday_flag', lit(1))
h = h.dropDuplicates()

cond = [df3['created_date'] == h['date_holiday']]
df3 = df3.join(h, cond, 'left').drop(h['date_holiday']).dropDuplicates()


# Numero de casos
# data base with number_cases
cd = df3.groupBy("created_date").count().orderBy('created_date')
cd = cd.withColumnRenamed("count", "number_cases")
cond = [df3['created_date'] == cd['created_date']]
# paste number_cases into data base
df3 = df3.join(cd, cond, 'left').drop(cd['created_date']).dropDuplicates()

# function to create historical variables
for i in range(1,history_days):
    var_name =  "number_cases_" + str(i) +"days_ago"
    cd2 = cd.withColumn('created_date_1', date_add(cd['created_date'], i))
    cd2 = cd2.withColumnRenamed("number_cases", var_name)
    cond = [df3['created_date'] == cd2['created_date_1']]
    df3 = df3.join(cd2, cond, 'left').drop(cd2['created_date']).dropDuplicates()
    df3 = df3.drop(cd2['created_date_1'])
