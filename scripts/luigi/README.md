<div class="tip" markdown="1">

# ETL- *Luigi Tasks*
<div align="justify">

Los *Luigi Tasks* descritos a continuación ejecutan las siguientes tareas de manera consecutiva y dependiente:

1) *downloadRawJSONData*: conexión a la API 311 NYC para extraer los datos y actualizaciones de la API en formato **.json**.

2) :extracción y guardado (en RDS) de los metadatos importantes asociados al task *downloadRawJSONData*.

3) *preprocParquetPandas*: preprocesamiento de los *.jsons* para convertirlos en formato *.parquet*.

4) :extracción y guardado (en RDS) de los metadatos asociados al task *preprocParquetPandas*.


## Ejecución de los *Tasks*

**1)** Nos conecamos al **EC2** (máquina es para el procesamiento de la información y es donde viven las tareas que realiza luigi); para tener acceso a la misma tenemos que hacer un secure copy de la *llave.pem* al bastion:

```
ssh -i  /tu/ruta/llave-bastion.pem /tu/ruta/llave/ec2/llave.pem ubuntu@ip-ec2:/home/ubuntu/.ssh
```


**2)** Una vez dentro del EC2, para ejecutar la tarea de descarga de detos RAW hacia la `S3` se ejecuta:

```
PYTHONPATH="." AWS_PROFILE=luigi_dpa luigi --module luigi_tasks_S3 preprocParquetSpark --local-scheduler --bucket prueba-nyc311 --year 2020 --month 3 --day 10
```

En este ejemplo en particular, estamos pasando los parámetros: year `<2020>`, month `<3>`, day `<10>`, bucket_name `<prueba-nyc311>`. Los mismos deberán ser modificados según se requiera. Por default, el buck_name es prueba-nyc311.

**Nota:** Como los tasks son dependendientes entre sí, basta con ejecutar el último tasks para que los tasks precedentes se ejecuten, en caso no haber sido ejecutados previamente.










</div>
