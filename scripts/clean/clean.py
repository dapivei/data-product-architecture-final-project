import pyspark
from pyspark.sql import functions as F


# Importamos la información
df = spark.read.parquet("s3://prueba-nyc311/preprocess/*/*/*/data_*_*_*.parquet")

# Hacemos una copia del dataframe original
df2 = df


# Corregimos valores nulos
def as_null(x):
    return F.when(F.col(x).isin('N/A', 'nan', 'NaN', 'n/a', 'Na', ''), None).otherwise(F.col(x))


cols_str = [item[0] for item in df2.dtypes if item[1].startswith('string')]
for i in cols_str:
    df2 = df2.withColumn(i, as_null(i))



# Corregimos el tipo de las variables
timeFmt = "yyyy-MM-dd'T'HH:mm:ss.SSS"
created_date = F.unix_timestamp('created_date', format=timeFmt)
closed_date = F.unix_timestamp('closed_date', format=timeFmt)
resolution_action_updated_date = F.unix_timestamp('resolution_action_updated_date', format=timeFmt)
due_date = F.unix_timestamp('due_date', format=timeFmt)

df2 = df2.withColumn("created_date_timestamp", created_date)
df2 = df2.withColumn("closed_date_timestamp", closed_date)
df2 = df2.withColumn("resolution_action_updated_date_timestamp", resolution_action_updated_date)
df2 = df2.withColumn("due_date_timestamp", due_date)


created_date = F.from_unixtime(F.unix_timestamp(df2['created_date'],"yyyy-MM-dd'T'HH:mm:ss.SSS"),"yyyy-MM-dd").alias("created_date").cast("date")
closed_date = F.from_unixtime(F.unix_timestamp(df2['closed_date'],"yyyy-MM-dd'T'HH:mm:ss.SSS"),"yyyy-MM-dd").alias("closed_date").cast("date")
resolution_action_updated_date = F.from_unixtime(F.unix_timestamp(df2['resolution_action_updated_date'],"yyyy-MM-dd'T'HH:mm:ss.SSS"),"yyyy-MM-dd").alias("resolution_action_updated_date").cast("date")
due_date =  F.from_unixtime(F.unix_timestamp(df2['due_date'],"yyyy-MM-dd'T'HH:mm:ss.SSS"),"yyyy-MM-dd").alias("due_date").cast("date")

df2 = df2.withColumn("created_date", created_date)
df2 = df2.withColumn("closed_date", closed_date)
df2 = df2.withColumn("resolution_action_updated_date", resolution_action_updated_date)
df2 = df2.withColumn("due_date", due_date)



# Cambiamos las variables string a lowercase
cols_str = [item[0] for item in df2.dtypes if item[1].startswith('string')]
for c in cols_str:
    col = F.lower(df2[c]).alias(c)
    df2 = df2.withColumn(c, col)



# Eliminamos registros repetidos
df2 = df2.dropDuplicates()

# Eliminamos registros que tengan puros valores nulos
df2 = df2.dropna(how='all')
