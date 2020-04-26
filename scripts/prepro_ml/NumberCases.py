#!/usr/bin/env python
# Calculate history of number of cases
from pyspark.sql.functions import date_add
import pyspark
history_days = 10

df = spark.read.parquet("s3://prueba-nyc311/cleaned /*/*/*/data_*_*_*.parquet")
df3 = df
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
