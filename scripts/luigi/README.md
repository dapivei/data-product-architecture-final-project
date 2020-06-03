<div class="tip" markdown="1">

# <em>Luigi Tasks</em><div align="justify">

Los <em>Luigi Tasks</em> descritos a continuación ejecutan las siguientes tareas de manera consecutiva y dependiente:

- <em>Task<em>10_download</em>: conexión a la API 311 NYC para extraer los datos y actualizaciones de la API en formato <strong>.json</strong>. Los datos son guardados en S3.

- <em>Task_11_metaDownload</em>:extracción y guardado (en RDS) de los metadatos importantes asociados al task <em>Task_10_download</em> en la tabla <em>raw.etl_execution</em>.

- <em>Task_20_preproc</em>: Convertir datos descargados en JSON y los transforma a formato parquet utilizando pandas. <em>.parquet</em>. Los datos son guardados en S3.

- <em>Task_21_metaPreproc</em>:extracción y guardado (en RDS) de los metadatos asociados al task <em>Task_20_preproc</em>.

- <em>Task_30_cleaned</em>: limpia y estandariza los datos que se tienen en parquet utilizando pandas.  Los datos son guardados en S3.

- <em>Task_31_metaClean</em>:extracción y guardado (en RDS) de los metadatos asociados al task <em>Task_30_cleaned</em> en la tabla <em>cleaned.etl_execution</em>.

- <em>Task_32_cleaned_UnitTest</em>: Test unitario en <code>marbles</code> para los datos contenidos en el <em>schema</em> cleaned.

- <em>Task_33_metaCleanUT</em>: extracción y guardado (en RDS) de los metadatos asociados al task <em>Task_31_metaClean</em> en la tabla <em>cleaned.ut_execution</em>. Solo guarda metadatos en caso de que los datos superen la prueba unitaria de <em>Task_32_cleaned_UnitTest</em>.


- <em>Task_40_mlPreproc</em>: Utilizando los datos limpios cuenta los registros por fecha y colapsa en una sola tabla las columnas created_date y numero de registros.

- <em>Task_50_ml</em>: Utilizando los datos generados en <em>Task_40_mlPreproc</em> hace el procesamiento de los datos que serán utilizandos con la librería <code>sklearn</code> de <code>Python</code>

- <em>Task_51_feature_UnitTest</em>: Realiza dos test unitarios en <code>marbles</code> a la matriz de datos generada en  <em>Task_50_ml</em>.

- <em>Task_52_metaFeatureEngUTM</em>: extracción y guardado (en RDS) de los metadatos asociados al task <em>Task_51_feature_UnitTest</em> en la tabla <em>mlpreproc.ut_execution</em>. Solo se guardan metadatos en caso de que se supere la prueba unitaria.

- <em>Task_53_feature_PandasTest</em>: Realiza un test unitario en <code>pandas</code> a la matriz de datos generada en  <em>Task_50_ml</em>.

- <em>Task_54_metaFeatureEngUTP(CopyToTable):</em>: extracción y guardado (en RDS) de los metadatos asociados al task <em>Task_53_feature_PandasTest</em> en la tabla <em>mlpreproc.ut_execution</em>. Solo se guardan metadatos en caso de que se supere la prueba unitaria.

- <em>Task_60_Train</em>: Se entrena un modelo de RFC en <code>sklearn</code> y se guarda el <code>pickle</code> asociado en S3\. En el futuro se va a variar la gamma de los modelos y los hiper-parámetros asociados. Además de incorporar un <em>magic-loop</em> que seleccione el mejor modelo de acuerdo a una métrica de desempeño de interés.

- <em>Task_61_metaModel</em>: extracción y guardado (en RDS) de los metadatos asociados al task <em>Task_60_Train</em> en la tabla <em>modeling.ejecucion</em>.

- <em>Task_70_biasFairness</em>: Genera métricas de <em>Bias &amp; Fairness</em> por grupos de distrito utilizando la librería <code>aequitas</code>. Guarda la tabla con la métricas asociadas en S3.

- <em>Task_71_biasFairnessRDS</em>: Guarda las métricas generadas en <em>Task_70_biasFairness</em> en RDS en la tabla <em>biasfairness.aequitas_ejecucion</em>.

- <em>Task_72_metabiasFairness</em>: extracción y guardado (en RDS) de los metadatos asociados al task <em>Task_70_biasFairness</em> en la tabla <em>Task_71_biasFairnessRDS</em>.

- <em>Task_80_Predict</em>: Utiliza el modelo pre-entrenado seleccionado para hacer predicciones por fecha (diaria) y por distrito de los casos de ruido en Nueva York. Guarda los datos en S3 en formato <strong>.parquet</strong>.

- <em>Task_81_metaPredict</em>: extracción y guardado (en RDS) de los metadatos asociados al task <em>Task_80_Predict</em> en la tabla <em>prediction.ejecucion</em>.

- <em>Task_82_testPrediction</em>:Realiza un test unitario en las predicciones generadas en  <em>Task_81_metaPredict</em> utilizando la librería <code>marbles</code>.

- <em>Task_83_metaTestPredictions</em>: extracción y guardado (en RDS) de los metadatos asociados al task <em>Task_82_testPrediction</em> en la tabla <em>prediction.ut_execution</em>.


## Ejecución de los <em>Tasks</em>

<strong>1)</strong> Nos conecamos al <strong>EC2</strong> (máquina es para el procesamiento de la información y es donde viven las tareas que realiza luigi); para tener acceso a la misma tenemos que hacer un secure copy de la <em>llave.pem</em> al bastion:

<code>ssh -i  /tu/ruta/llave-bastion.pem /tu/ruta/llave/ec2/llave.pem ubuntu@ip-ec2:/home/ubuntu/.ssh</code>


<strong>2)</strong> Una vez dentro del EC2 (que cuente con permisos para acceder a la RDS y siendo un usuario autorizado), en el ambiente virtual con la paquetería necesaria -disponble <a href="https://github.com/dapivei/data-product-architecture-final-project/blob/master/requirements.txt">aquí</a>- en la línea de comandos podemos ejecutar la predicción de casos de ruido (última tarea del <code>pipeline</code> asociado), por ejemplo para 2020-5-12 de la siguiente forma asociado de la siguiente forma:

<code>PYTHONPATH="." AWS_PROFILE=luigi_dpa luigi --module luigi_orchestration_ec2 Task_83_metaTestPredictions --predictDate  2020/5/12</code>


<strong>Nota:</strong> Como los tasks son dependendientes entre sí, basta con ejecutar el último <em>task</em> para que los <em>tasks</em> precedentes se ejecuten, en caso no haber sido ejecutados previamente.

</em></div></div>

__
