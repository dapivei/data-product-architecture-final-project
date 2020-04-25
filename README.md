<div class="tip" markdown="1">


# 311 NYC Service Request Web-Service
***
<div align="justify">

### Integrantes:

- Cadavid-Sánchez Sebastián, [C1587S](https://github.com/C1587S)
- Herrera Musi Juan Pablo, [Pilo1961](https://github.com/Pilo1961)
- Paz Cendejas Francisco, [MrFranciscoPaz](https://github.com/MrFranciscoPaz)
- Villa Lizárraga Diego M., [dvilla88](https://github.com/dvilla88)
- Pinto Veizaga Daniela, [dapivei](https://github.com/dapivei)

### Contenidos de sitio:

- [X] I. Introducción

- [X] II. Problema

- [X] III. Objetivos del producto de datos

- [X] IV. Población Objetivo

- [X] V. Data Product Arquitecture

- [ ] VI. Métricas de Desempeño

- [X] VII. Set de datos

- [X] VIII. Solución Propuesta: Producto final

- [ ] IX. Modelos utilizados

- [ ] X. Implementación

- [ ] XI. Conclusión



### I. Introducción:

El gobierno de Nueva York, con el fin de proveer a la comunidad *newyorkina* con acceso directo a los servicios gubernamentales  y mejorar el seguimiento y control de los servicios gubernamentales, implementó el servicio de petición *NYC311*, disponible las 24 horas del día, los 7 días de la semana, los 365 días del año. De esta manera, según el portal web principal de [*NYC311*](https://portal.311.nyc.gov/about-nyc-311/), la misión del servicio de petición es:

>* "es proporcionar al público un acceso rápido y fácil a todos los servicios e información del gobierno de la ciudad de Nueva York al tiempo que ofrece el mejor servicio al cliente. Ayudamos a las agencias a mejorar la prestación de servicios permitiéndoles centrarse en sus misiones principales y administrar su carga de trabajo de manera eficiente. También proporcionamos información para mejorar el gobierno de la Ciudad a través de mediciones y análisis precisos y consistentes de la prestación de servicios".

**Gráfica 1.Portal-Web "NYC311 Service Request**

<p align="center">

<image width="300" height="330" src="./images/nyc_311_sr_website.png">

</p>

### II. Problema

Existe una brecha, aparentemente "infranqueable" entre el Estado y la ciudadanía, dónde los ciudadanos carecen de herramientas adecuadas para monitorear, participar y colaborar en el quehacer público. En este sentido la línea de peticiones NYC311 es una iniciativa para conectar el quehacer gubernamental con los ciudadanos a través de un línea disponible para levantar quejas y peticiones a las diferentes agencias gubernamentales. Sin embargo, este servicio aún es incipiente en el sentido que el ciudadano, hasta el momento, no cuenta con una herramienta eficaz de seguimiento a sus requerimientos, mediante la dotación de un tiempo estimado de resolución de su petición y de una métrica de control de tiempo estimado de respuesta, en comparación con otras peticiones de índole similar.


### III. Objetivos del producto de datos

El desarrollo de este producto de datos tiene los siguientes objetivos:

#### Objetivo generales

* Proveer una herramienta a las agencias gubernamentales para conocer con anticipación momentos en que se dará un exceso de demanda de servicios con el fin de tener una planeación y asignación adecuada.

#### Objetivos específicos

* Pronosticar días en que la demanda de recursos por agencia esté supere un límite establecido;

* Medir la divergencia en el tiempo de respuesta por distritos de la ciudad, por agencia y por tipo de solicitud.

#### Predicción:

- Predicción binaria que indica si el número de servicios requeridos por agencia gubernamental superará un límite previamente establecido.

- Re-entrenamiento 6 meses aproximadamente, luego de evaluar las métricas del modelo.

### IV. Población objetivo

- Agencias gubernamentales de la ciudad de NY que deseen tener una herramienta de predicción sobre el exceso de demanda de sus servicios.

<p align="center">


<image width="50" height="50" src="./images/warning_sign.png"> Implicaciones Éticas


</p>

- Las predicciones pueden estar sesgadas hacia las zonas (distritos) con mayor número de *service requests*;
- La credibilidad de las agencias públicas puede ser afectada por predicciones erróneas de tiempos de respuestas a los *service request*;
- Ciertas zonas y distritos pueden ser marginados o rezagados por lo posible reubicación de recursos gubernamentales para atender mejor a los requerimientos en las llamadas de los ciudadanos. Estas decisiones podrían ser derivadas del *output* del producto de datos;
- El producto de datos puede dar juicios de valor con respecto a la asignación de servicios de las agencias. Sin embargo, también se debe tener en cuenta que puede haber información omitida por el modelo que se tiene al interior de las agencias cuando estas toman las decisiones;
- Las agencias asignarán recursos para habilitar operaciones que cubran la demanda predicha, en caso de falsos positivos estos recursos serán desperdiciados.  



### V. Data Product Architecture:

**Gráfica 2.Data Product Pipeline: Deployment**

<p align="center">

<image width="700" height="100" src="./images/deployment.png">

</p>  

**Gráfica 3.Data Product Pipeline: Desarrollo y Producción**

<p align="center">

<image width="700" height="100" src="./images/desarrollo_produccion.png">

</p>  

**Extract, Transform and Load (ETL)**
##### Deploy
1. Script en python que hace peticiones a la API de **socrata** usando la librería **sodapy** solicitando todos los registros de la base de datos en formato JSON.
2. En la EC2 se recibe el archivo de datos y se guarda en la instancia S3. El script hace las predicciones por fecha y los archivos respuesta se van guardando en una estructura de carpetas dentro de S3. El S3 está encriptado de tal manera que solamente pueden ingresar con las credenciales asignadas.
3. Alimentamos el esquema **preprocessed** en el cual se genera la misma estructura de carpetas y los archivos JSON se guardan en formato parquet.
4. Se genera el esquema **cleaned** a partir del esquema preprocessed. En este esquema se eliminan las columnas que no tienen variabilidad o son en su mayoría valores nulos. Se asignan los tipos de dato a cada columna y se limpian acentos y caracteres extraños de nombres de columnas y observaciones. Se conserva el formato parquet.


Los detalles de la configuración y uso de la arquitura utilizada en AWS se encuenta en [scripts aws](https://github.com/dapivei/data-product-architecture-final-project/tree/master/scripts/conf-AWS "aws configuración")

##### Desarrollo
###### Extract
1. Hacemos una petición a la API solicitando los datos en formato JSON por medio de un script en python. Se hará un petición al día en la que filtramos para incluir solamente los registros que se crearon el día anterior (registros nuevos). La petición actualmente se hace por número de registros. Se modificará para realizar un filtrado utilizando la variable **created_date**.
2. Hacemos una segunda petición a la API solicitando los registros que se cerraron el día anterior en formato JSON. Filtramos la petición por medio de las variables **closed_date** (actualización de registros existentes).

##### Load
1. La petición de los datos en *extract* nos permite obtener los archivos JSON de la API, uno por cada petición y los registros nuevos se incorporan a la estructura de carpetas en S3 con la fecha de creación del registro (futura estructura). Los registros obtenidos por la variable closed_date se guardan en otra estructura de carpatas generada a partir de las fechas.

##### Transform
1. Se corre un script en python (pyspark) que hace consultas a los datos almacenados en parquet, se limpian los datos, se quitan las columnas nulas o que no tienen variabilidad y incorporan los registros al esquema cleaned de la estructura de carpetas.  
2. Se corre un segundo script en python que transforma el archivo JSON que tiene la actualización de registros existentes a parquet, se lleva al formato cleaned y es guarda en la estructura de carpetas.

**Gráfica 4.Extract, Load & Transform**

<p align="center">

<image width="700" height="500" src="./images/elt_aws.png">

</p>  


#### Linaje de Datos: Metadatos y Diseño de Tablas(RDS) para la fase del *Extract, Load and Transform(ETL)*

****
##### Objetivo:

Presentación de los esquemas que representan el linaje de datos del ETL del proyecto "311 NYC SR".
****

<div align="justify">


##### Raw

> **Descripción:**
**1)** Petición a la [API](https://data.cityofnewyork.us/Social-Services/311-Service-Requests-from-2010-to-Present/erm2-nwe9) los datos, filtrados por fecha.
**2)** Guardado de los datos en formato original (json) dentro de un bucket en `S3`. Nota: Se requieren todas las variables con las que cuenta la base de datos.  

> **Metadata asociada**:

+ Fecha de ejecución
+ Hora de ejecución
+ Parámetros con los que ejecutaste tu task. Lista en formato JSON.
    + year
    + month
    + day
    + bucket_name
+ Quién ejecutó el task (usuario)
+ Desde donde se ejecutó (ip, instancia EC2)
+ Número de registros ingestados
+ Nombre del archivo generado
+ Ruta de almacentamiento en S3 (incluyendo el bucket)
+ Usuario en BD
+ Variables (en el orden en el que aparecen)
+ Tipos de datos **Hay que sacar todos en tipo texto y luego asignamos tipos**
+ Qué script se ejecutó (tag de github): **este paso no es trivial**.
+ Logs del script
+ Estatus de ejecución

##### Preprocessed

>**Descripción**: Cambio del formato original (json) al formato de procesamiento (parquet).

> **Metadata asociada**:
+ Fecha de ejecución
+ Hora de ejecución
+ Quién ejecutó (usuario)
+ Cambio de formato, de JSON a Parquet.
+ Tag de código de github que se ejecutó
+ Dónde se ejecutó
+ Idealmente # de registros modificados
+ Logs del scrpit
+ Estatus de ejecución: Fallido, exitoso, etc.

##### Clean

>**Descripción**: **1)** Limpieza de títulos de las columnas, por ejemplo: pasar los datos a minúsculas y quitar caracteres especiales. **2)** Asignación del tipo de dato a las columnas. **3)** Eliminación de las columnas vacías o sin variabilidad.


>**Metadata asociada**:

+ Fecha de ejecución
+ Quién ejecutó (usuario)
+ Dónde se ejecutó
+ Descripción de transformaciones a cada columna
+ Tag de código de github que se ejecutó
+ Número de registros modificados
+ Logs del script (estatus de ejecución por columna)
+ Estatus de ejecución

##### Semantic

> **Descripción:** Las entidades son las agencias; los eventos son los requerimentos de servicio que se tienen. Haremos la predicción de si al día siguiente (granularidad por definir) el número de eventos será mayor que un límite establecido previamente (promedio o por definir). En este paso se generan nuevas variables relevantes para el análisis.

> **Metadata asociada:**
+ Fecha de ejecución
+ Quién ejecutó (usuario)
+ Dónde se ejecutó
+ Nombre de archivos creados
+ Ruta donde se guardaron
+ Descripción de transformaciones a cada columna
+ Descripción de nuevas variables
+ Tag de código de github que se ejecutó
+ Parámetros del script
+ Número de registros modificados
+ Logs del script (estatus de ejecución por columna)
+ Estatus de ejecución

##### ML preprocessing

Se le hacen las transformaciónes a los datos con el fin de ponerlos en formato y poderlos procesar con algoritmos de machine learning. Se ponen etiquetas numpericas a las variables categóricas y one hot encoding.


> **Metadata Asociada**:
+ Fecha de ejecución
+ Quién ejecutó (usuario)
+ Dónde se ejecutó
+ Nombre de archivos creados
+ Ruta donde se guardaron
+ Descripción de transformaciones a cada columna
+ Tag de código de github que se ejecutó
+ Parámetros del script
+ Número de registros modificados
+ Logs del script (estatus de ejecución por columna)
+ Estatus de ejecución

### VI. Métricas de Desempeño

### VII. Set de datos

+ unique key
+ created date
+ updated
+ resolution
+ closed
+ due date
+ agency
+ complaint type
+ demographic variables
+ status

#### Frecuencia de actualización de datos

- Los datos en la API se actualizan diariamente con un día de rezago (hoy se actualizan los datos de ayer). Nosotros descargaremos los datos diariamente desde la API utilizando `CRON` integrado con una tarea en `luigi`.
- Cada 40 días se realiza una actualización de la base de datos (para poner al día el estatus del *service request*).
- La descarga de datos se realizará diariamente a las 2:00 a.m. Actualmente, la estructura de carpetas agrega los datos de manera agregada.
_En fase de modificación_: se establecerá una estructura de descarga de los datos por día, en el cual se contienen todos los *service requests* de la fecha.

**Para tener en cuenta:**

- Aparentemente, la carga de datos a la API solo se genera en días hábiles. Esto puede sesgar las entradas del modelo en la medida que los lunes serían los días con un mayor número de consultas (Falta identificar en el EDA). Esto podría afectar las predicciones.

### VIII. Solución Propuesta: Producto Final



**Gráfica 5.Portal-Web "NYC311 Service Request Engagement"**

<p align="center">

<image width="350" height="250" src="./images/web_service_proposal.png">

</p>

El producto de datos va a ser un dashboard que genere predicciones diarias de los *service request recibidos* una vez se realice la ingesta de datos. El dashboard va a permitir filtrar las predicciones por fecha de creación, días para completar, distrito, agencia, tipo de *service request*.


### IX. Modelos utilizados

### X. Implementación


### XI. Conclusión

</div>
