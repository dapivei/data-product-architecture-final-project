
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
