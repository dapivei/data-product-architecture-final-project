
class Task_20_metaDownload(luigi.task.WrapperTask):
    '''
    Guardar los metadatos de la descarga de datos del schema RAW
    Son guardados en la base de datos nyc311_metadata en la tabla raw.etl_execution
    '''
    # ==============================
    # parametros:
    # ==============================
    bucket = luigi.Parameter(default="prueba-nyc311")
    year = luigi.Parameter()
    month = luigi.Parameter()
    day = luigi.Parameter()
    # ==============================

    def requires(self):
        return Task_10_download(year=self.year, month=self.month, day=self.day)

    def run(self):
        # se instancia la clase raw_metadata()
        cwd = os.getcwd()  # directorio actual
        raw_meta = preproc_metadata()
        raw_meta.name = f"data_{self.year}_{self.month}_{self.day}"
        raw_meta.user = str(getpass.getuser())
        raw_meta.machine = str(platform.platform())
        raw_meta.ip = execv("curl ipecho.net/plain ; echo", cwd)
        raw_meta.creation_date = str(datetime.datetime.now())
        raw_meta.location = f"{path_raw}/{raw_meta.name}"
        raw_meta.param_year = str(self.year)
        raw_meta.param_month = str(self.month)
        raw_meta.param_day = str(self.day)
        raw_meta.param_bucket = str(self.bucket)

        ubicacion_completa = f"{raw_meta.location}.json"
        meta = raw_meta.info()  # extrae info de la clase

        print("=" * 100)
        print(meta)
        print("complete name: ", ubicacion_completa)
        print("name: ", raw_meta.name)
        print("extensión: ", raw_meta.extention)
        print("schema: ", raw_meta.schema)
        print("tamaño: ", raw_meta.size)
        print("action: ", raw_meta.action)
        print("usuario: ", raw_meta.user)
        print("maquina: ", raw_meta.machine)
        print("ip: ", raw_meta.ip)
        print("fecha de creación: ", raw_meta.creation_date)
        print("ubicación: ", raw_meta.location)
        print("param [year]: ", raw_meta.param_year)
        print("param [month]: ", raw_meta.param_month)
        print("param [day]: ", raw_meta.param_day)
        print("param [bucket]: ", raw_meta.param_bucket)
        print("=" * 100)

        # conectarse a la base de datos y guardar a esquema raw.etl_execution
        conn = ps.connect(host=settings.get('host'),
                          port=settings.get('port'),
                          database="nyc311_metadata",
                          user=settings.get('usr'),
                          password=settings.get('password'))
        cur = conn.cursor()
        columns = "(name, extention, schema, action, creator, machine, ip, creation_date, size, location,status, param_year, param_month, param_day, param_bucket)"
        sql = "INSERT INTO raw.etl_execution" + columns + \
            " VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);"
        cur.execute(sql, meta)
        conn.commit()
        cur.close()
        conn.close()

class Task_40_metaPreproc(luigi.task.WrapperTask):
    '''
    Guardar los metadatos de la descarga de datos del schema RAW
    Son guardados en la base de datos nyc311_metadata en la tabla raw.etl_execution
    '''
    # ==============================
    # parametros:
    # ==============================
    bucket = luigi.Parameter(default="prueba-nyc311")
    year = luigi.Parameter()
    month = luigi.Parameter()
    day = luigi.Parameter()
    # ==============================

    def requires(self):
        return Task_30_preproc(year=self.year, month=self.month, day=self.day)

    def run(self):
        # se instancia la clase raw_metadata()
        cwd = os.getcwd()  # directorio actual
        raw_prep = preproc_metadata()
        raw_prep.name = f"data_{self.year}_{self.month}_{self.day}"
        raw_prep.user = str(getpass.getuser())
        raw_prep.machine = str(platform.platform())
        raw_prep.ip = execv("curl ipecho.net/plain ; echo", cwd)
        raw_prep.creation_date = str(datetime.datetime.now())
        raw_prep.location = f"{path_raw}/{raw_prep.name}"
        raw_prep.param_year = str(self.year)
        raw_prep.param_month = str(self.month)
        raw_prep.param_day = str(self.day)
        raw_prep.param_bucket = str(self.bucket)

        ubicacion_completa = f"{raw_prep.location}.json"
        meta = raw_prep.info()  # extraer información de la clase

        print("=" * 100)
        print(meta)
        print("complete name: ", ubicacion_completa)
        print("name: ", raw_prep.name)
        print("extensión: ", raw_prep.extention)
        print("tamaño: ", raw_prep.size)
        print("action: ", raw_prep.action)
        print("usuario: ", raw_prep.user)
        print("maquina: ", raw_prep.machine)
        print("ip: ", raw_prep.ip)
        print("fecha de creación: ", raw_prep.creation_date)
        print("ubicación: ", raw_prep.location)
        print("param [year]: ", raw_prep.param_year)
        print("param [month]: ", raw_prep.param_month)
        print("param [day]: ", raw_prep.param_day)
        print("param [bucket]: ", raw_prep.param_bucket)
        print("=" * 100)

        # conectarse a la base de datos y guardar a esquema raw.etl_execution
        conn = ps.connect(host=settings.get('host'),
                          port=settings.get('port'),
                          database="nyc311_metadata",
                          user=settings.get('usr'),
                          password=settings.get('password'))
        cur = conn.cursor()
        columns = "(name, extention, schema, action, creator, machine, ip, creation_date, size, location,status, param_year, param_month, param_day, param_bucket)"
        sql = "INSERT INTO preprocessed.etl_execution" + columns + \
            " VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);"

        cur.execute(sql, meta)
        conn.commit()
        cur.close()
        conn.close()


class Task_51_metaClean(luigi.task.WrapperTask):
    '''
    Guardar los metadatos de la descarga de datos del schema cleaned
    Son guardados en la base de datos nyc311_metadata en la tabla clean.etl_execution
    '''
    # ==============================
    # parametros:
    # ==============================
    bucket = luigi.Parameter(default="prueba-nyc311")
    year = luigi.Parameter()
    month = luigi.Parameter()
    day = luigi.Parameter()
    # ==============================

    def requires(self):
        return Task_50_cleaned(year=self.year, month=self.month, day=self.day)

    def run(self):
        # se instancia la clase raw_metadata()
        cwd = os.getcwd()  # directorio actual
        raw_prep = cleaned_metadata()
        raw_prep.name = f"data_{self.year}_{self.month}_{self.day}"
        raw_prep.user = str(getpass.getuser())
        raw_prep.machine = str(platform.platform())
        raw_prep.ip = execv("curl ipecho.net/plain ; echo", cwd)
        raw_prep.creation_date = str(datetime.datetime.now())
        raw_prep.location = f"{path_raw}/{raw_prep.name}"
        raw_prep.param_year = str(self.year)
        raw_prep.param_month = str(self.month)
        raw_prep.param_day = str(self.day)
        raw_prep.param_bucket = str(self.bucket)

        ubicacion_completa = f"{raw_prep.location}.json"
        meta = raw_prep.info()  # extraer información de la clase

        print("=" * 100)
        print(meta)
        print("complete name: ", ubicacion_completa)
        print("name: ", raw_prep.name)
        print("extensión: ", raw_prep.extention)
        print("tamaño: ", raw_prep.size)
        print("action: ", raw_prep.action)
        print("usuario: ", raw_prep.user)
        print("maquina: ", raw_prep.machine)
        print("ip: ", raw_prep.ip)
        print("fecha de creación: ", raw_prep.creation_date)
        print("ubicación: ", raw_prep.location)
        print("param [year]: ", raw_prep.param_year)
        print("param [month]: ", raw_prep.param_month)
        print("param [day]: ", raw_prep.param_day)
        print("param [bucket]: ", raw_prep.param_bucket)
        print("=" * 100)

        # conectarse a la base de datos y guardar a esquema raw.etl_execution
        conn = ps.connect(host=settings.get('host'),
                          port=settings.get('port'),
                          database="nyc311_metadata",
                          user=settings.get('usr'),
                          password=settings.get('password'))
        cur = conn.cursor()
        columns = "(name, extention, schema, action, creator, machine, ip, creation_date, size, location,status, param_year, param_month, param_day, param_bucket)"
        sql = "INSERT INTO cleaned.etl_execution" + columns + \
            " VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);"

        cur.execute(sql, meta)
        conn.commit()
        cur.close()
        conn.close()


class Task_53_metaCleanUT(luigi.task.WrapperTask):
    '''
    Guardar los metadatos de la descarga de datos del schema cleaned
    Son guardados en la base de datos nyc311_metadata en la tabla clean.etl_execution
    '''
    # ==============================
    # parametros:
    # ==============================
    bucket = luigi.Parameter(default="prueba-nyc311")
    year = luigi.Parameter()
    month = luigi.Parameter()
    day = luigi.Parameter()
    # ==============================

    def requires(self):
        return Task_52_cleaned_UnitTest(year=self.year, month=self.month, day=self.day)

    def run(self):
        # se instancia la clase raw_metadata()
        cwd = os.getcwd()  # directorio actual
        cleanUT = cleaned_metadataUnitTest()
        cleanUT.name = f"data_{self.year}_{self.month}_{self.day}"
        cleanUT.user = str(getpass.getuser())
        cleanUT.machine = str(platform.platform())
        cleanUT.ip = execv("curl ipecho.net/plain ; echo", cwd)
        cleanUT.creation_date = str(datetime.datetime.now())
        cleanUT.location = f"{path_raw}/{cleanUT.name}"
        cleanUT.param_year = str(self.year)
        cleanUT.param_month = str(self.month)
        cleanUT.param_day = str(self.day)
        cleanUT.param_bucket = str(self.bucket)

        ubicacion_completa = f"{cleanUT.location}.json"
        meta = cleanUT.info()  # extraer información de la clase

        # conectarse a la base de datos y guardar a esquema raw.etl_execution
        conn = ps.connect(host=settings.get('host'),
                          port=settings.get('port'),
                          database="nyc311_metadata",
                          user=settings.get('usr'),
                          password=settings.get('password'))
        cur = conn.cursor()
        columns = "(name, extention, schema, action, creator, machine, ip, creation_date, size, location,status, param_year, param_month, param_day, param_bucket)"
        sql = "INSERT INTO cleaned.ut_execution" + columns + \
            " VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);"
        cur.execute(sql, meta)
        conn.commit()
        cur.close()
        conn.close()
@Task_53_metaCleanUT.event_handler(luigi.Event.SUCCESS)
def celebrate_success(task):
    print(u'\u2705'*2)

class Task_71_mlPreproc_firstTime(luigi.Task):
    '''
    Contar los registros por fecha y colapsar en una sola tabla que contendra las columnas de created_date y numero de registros
    '''
    # ==============================
    # parametros:
    # ==============================
    bucket = luigi.Parameter(default="prueba-nyc311")
    year = luigi.Parameter()
    month = luigi.Parameter()
    day = luigi.Parameter()

    # ==============================
    def requires(self):
        return Task_51_metaClean(year=self.year, month=self.month, day=self.day)

    def output(self):
        # guarda los datos en s3://prueba-nyc311/raw/.3..
        output_path = f"s3://{self.bucket}/mlpreproc/mlPreproc.parquet"
        return luigi.contrib.s3.S3Target(path=output_path)

    def run(self):
        import io
        from datetime import datetime, timedelta
        # Autenticación en S3
        ses = boto3.session.Session(
            profile_name='luigi_dpa', region_name='us-west-2')
        buffer=io.BytesIO()
        s3_resource = ses.resource('s3')
        obj = s3_resource.Bucket(name=self.bucket)

        end_date=datetime(int(self.year),int(self.month),int(self.day))
        start_date= datetime(2009,12,31)
        date=start_date

        flag=0
        count=0
        while(date<end_date):
            date=date+timedelta(days=1)

            #lectura de datos
            try:
                key = f"cleaned/{date.year}/{date.month}/{date.day}/data_{date.year}_{date.month}_{date.day}.parquet"
                parquet_object = s3_resource.Object(bucket_name=self.bucket, key=key) # objeto
                data_parquet_object = io.BytesIO(parquet_object.get()['Body'].read())
                df = pd.read_parquet(data_parquet_object)
            except:
                #para generar metadata
                print(date)
                continue

            # Filtramos los casos de ruido para la agencia NYPD
            df=df.loc[(df["agency"]=='nypd') & (df["complaint_type"].str.contains("noise")),:]
            df=df.reset_index(drop=True)

            #cuenta los registros y colapsa el df
            df['counts']=1
            df=df.loc[:,['created_date','counts','borough']]
            df=df.groupby(['created_date','borough'],as_index=False).count()

            #create or append df
            if(flag==0):
                df2=df
                flag=1
            else:
                #pegamos los dataframes
                df2=df2.append(df)

            del(df)

            #keep track of progress
            count=count+1
            if(count%100==0):
                print(count)
            #aumentamos un dia

            #print(date)

        df2.drop_duplicates(inplace=True)
        df2=df2.reset_index(drop=True)
        #print(df2)
        #pasa a formato parquet
        df2.to_parquet(self.output().path, engine='auto', compression='snappy')


# class Task_72_metaMlPreproc(luigi.task.WrapperTask):
#     '''
#     Guardar los metadatos de mlPreproc
#     '''
#     # ==============================
#     # parametros:
#     # ==============================
#     bucket = luigi.Parameter(default="prueba-nyc311")
#     year = luigi.Parameter()
#     month = luigi.Parameter()
#     day = luigi.Parameter()
#     # ==============================
#
#     def requires(self):
#         return Task_71_mlPreproc_firstTime(year=self.year, month=self.month, day=self.day)
#
#     def run(self):
#         # se instancia la clase raw_metadata()
#         cwd = os.getcwd()  # directorio actual
#         raw_meta = mlPreproc_metadata()
#         raw_meta.name = f"data_{self.year}_{self.month}_{self.day}"
#         raw_meta.user = str(getpass.getuser())
#         raw_meta.machine = str(platform.platform())
#         raw_meta.ip = execv("curl ipecho.net/plain ; echo", cwd)
#         raw_meta.creation_date = str(datetime.datetime.now())
#         raw_meta.location = f"{path_raw}/{raw_meta.name}"
#         raw_meta.param_year = str(self.year)
#         raw_meta.param_month = str(self.month)
#         raw_meta.param_day = str(self.day)
#         raw_meta.param_bucket = str(self.bucket)
#
#         ubicacion_completa = f"{raw_meta.location}.json"
#         meta = raw_meta.info()  # extrae info de la clase
#
#         print("=" * 100)
#         print(meta)
#         print("complete name: ", ubicacion_completa)
#         print("name: ", raw_meta.name)
#         print("extensión: ", raw_meta.extention)
#         print("schema: ", raw_meta.schema)
#         print("tamaño: ", raw_meta.size)
#         print("action: ", raw_meta.action)
#         print("usuario: ", raw_meta.user)
#         print("maquina: ", raw_meta.machine)
#         print("ip: ", raw_meta.ip)
#         print("fecha de creación: ", raw_meta.creation_date)
#         print("ubicación: ", raw_meta.location)
#         print("param [year]: ", raw_meta.param_year)
#         print("param [month]: ", raw_meta.param_month)
#         print("param [day]: ", raw_meta.param_day)
#         print("param [bucket]: ", raw_meta.param_bucket)
#         print("=" * 100)
#
#         # conectarse a la base de datos y guardar a esquema raw.etl_execution
#         conn = ps.connect(host=settings.get('host'),
#                           port=settings.get('port'),
#                           database="nyc311_metadata",
#                           user=settings.get('usr'),
#                           password=settings.get('password'))
#         cur = conn.cursor()
#         columns = "(name, extention, schema, action, creator, machine, ip, creation_date, size, location,status, param_year, param_month, param_day, param_bucket)"
#         sql = "INSERT INTO mlpreproc.feature_engineering" + columns + \
#             " VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);"
#         cur.execute(sql, meta)
#         conn.commit()
#         cur.close()
#         conn.close()


class Task_83_metaFeatureEngUTM(luigi.task.WrapperTask):
    '''
    Guardar los metadatos de la descarga de datos del schema FE con marbles
    Son guardados en la base de datos nyc311_metadata en la tabla mlpreproc.ut_execution
    '''
    # ==============================
    # parametros:
    # ==============================
    bucket = luigi.Parameter(default="prueba-nyc311")
    year = luigi.Parameter()
    month = luigi.Parameter()
    day = luigi.Parameter()
    # ==============================

    def requires(self):
        return Task_82_feature_UnitTest(year=self.year, month=self.month, day=self.day)

    def run(self):
        # se instancia la clase raw_metadata()
        cwd = os.getcwd()  # directorio actual
        feUT = FE_metadataUnitTest()
        feUT.name = "ml"
        feUT.user = str(getpass.getuser())
        feUT.machine = str(platform.platform())
        feUT.ip = execv("curl ipecho.net/plain ; echo", cwd)
        feUT.creation_date = str(datetime.datetime.now())
        feUT.location = "ml/ml.parquet"
        feUT.param_bucket = str(self.bucket)
        # las pruebas unitarias que se superaron
        feUT.action = "unit test for feature engineering (marbles): test_created_date_year_vs_onehot & test_created_date_month_vs_onehot"

        ubicacion_completa = f"{feUT.location}.parquet"
        meta = feUT.info()  # extraer información de la clase

        # conectarse a la base de datos y guardar a esquema raw.etl_execution
        conn = ps.connect(host=settings.get('host'),
                          port=settings.get('port'),
                          database="nyc311_metadata",
                          user=settings.get('usr'),
                          password=settings.get('password'))
        cur = conn.cursor()
        columns = "(name, extention, schema, action, creator, machine, ip, creation_date, size, location,status, param_bucket)"
        sql = "INSERT INTO mlpreproc.ut_execution" + columns + \
            " VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);"
        cur.execute(sql, meta)
        conn.commit()
        cur.close()
        conn.close()
@Task_83_metaFeatureEngUTM.event_handler(luigi.Event.SUCCESS)
def celebrate_success(task):
    print(u'\u2705'*2, "Se guardaron los metadatos para UT con marbles.")



class Task_85_metaFeatureEngUTP(luigi.task.WrapperTask):
    '''
    Guardar los metadatos de la descarga de datos del schema FE para unit testing con pandas
    Son guardados en la base de datos nyc311_metadata en la tabla mlpreproc.ut_execution
    '''
    # ==============================
    # parametros:
    # ==============================
    bucket = luigi.Parameter(default="prueba-nyc311")
    year = luigi.Parameter()
    month = luigi.Parameter()
    day = luigi.Parameter()
    # ==============================

    def requires(self):
        return Task_84_feature_PandasTest(year=self.year, month=self.month, day=self.day)

    def run(self):
        # se instancia la clase raw_metadata()
        cwd = os.getcwd()  # directorio actual
        feUT = FE_metadataUnitTest()
        feUT.name = "ml"
        feUT.user = str(getpass.getuser())
        feUT.machine = str(platform.platform())
        feUT.ip = execv("curl ipecho.net/plain ; echo", cwd)
        feUT.creation_date = str(datetime.datetime.now())
        feUT.location = "ml/ml.parquet"
        feUT.param_bucket = str(self.bucket)
        # las pruebas unitarias que se superaron
        feUT.action = "unit test for feature engineering (pandas): prueba_casos_dia"

        ubicacion_completa = f"{feUT.location}.parquet"
        meta = feUT.info()  # extraer información de la clase

        # conectarse a la base de datos y guardar a esquema raw.etl_execution
        conn = ps.connect(host=settings.get('host'),
                          port=settings.get('port'),
                          database="nyc311_metadata",
                          user=settings.get('usr'),
                          password=settings.get('password'))
        cur = conn.cursor()
        columns = "(name, extention, schema, action, creator, machine, ip, creation_date, size, location,status, param_bucket)"
        sql = "INSERT INTO mlpreproc.ut_execution" + columns + \
            " VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);"
        cur.execute(sql, meta)
        conn.commit()
        cur.close()
        conn.close()
@Task_85_metaFeatureEngUTP.event_handler(luigi.Event.SUCCESS)
def celebrate_success(task):
    print(u'\u2705'*2, "Se guardaron los metadatos para UT con pandas.")


class Task_110_metaModel(luigi.task.WrapperTask):
    '''
    Guardar los metadatos del entrenamiento de modelos
    '''
    # ==============================
    # parametros:
    # ==============================
    bucket = luigi.Parameter(default="prueba-nyc311")
    nestimators =luigi.Parameter()
    maxdepth= luigi.Parameter()
    criterion=luigi.Parameter()
    mock= luigi.Parameter(default=1)
    #year = luigi.Parameter()
    #month = luigi.Parameter()
    #day = luigi.Parameter()


    def requires(self):
        return Task_100_Train(nestimators=self.nestimators, maxdepth=self.maxdepth, criterion=self.criterion)
        #return luigi.contrib.s3.exist(year=self.year, month=self.month, day=self.day)
        #return luigi.S3Target(f"s3://{self.bucket}/ml/ml.parquet")

    def run(self):
        import os
    # ==============================
    # se instancia la clase raw_metadata()
        cwd = os.getcwd()  # directorio actual
        model_meta = model_metadata()
        model_meta.model_name = f"depth{self.maxdepth}_{self.criterion}_estimatros{self.nestimators}.pickle"
        model_meta.creator = str(getpass.getuser())
        model_meta.machine = str(platform.platform())
        model_meta.ip = execv("curl ipecho.net/plain ; echo", cwd)
        model_meta.date = str(datetime.datetime.now())
        model_meta.location = f"s3://{self.bucket}/ml/modelos/depth{self.maxdepth}_{self.criterion}_estimatros{self.nestimators}.pickle"
        model_meta.max_depth = str(self.maxdepth)
        model_meta.criterion = str(self.criterion)
        model_meta.n_estimators = str(self.nestimators)


        ubicacion_completa = model_meta.location
        meta = model_meta.info()  # extrae info de la clas

        # conectarse a la base de datos y guardar a esquema raw.etl_execution
        conn = ps.connect(host=settings.get('host'),
                          port=settings.get('port'),
                          database="nyc311_metadata",
                          user=settings.get('usr'),
                          password=settings.get('password'))
        cur = conn.cursor()
        columns = "(model_name, model_type, schema, action, creator, machine, ip, date, location, status, max_depth, criterion, n_estimators, score_train)"
        sql = "INSERT INTO modeling.ejecucion" + columns + \
            " VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);"
        cur.execute(sql, meta)
        conn.commit()
        cur.close()
        conn.close()
