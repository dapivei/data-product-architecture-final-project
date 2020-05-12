import subprocess as sub # execv

from sodapy import Socrata # queryApi311
from dynaconf import settings # queryApi311

from datetime import date
from dynaconf import settings
from sodapy import Socrata

##########################
import datetime
import getpass
import platform
import os
import socket
#from functionsV2 import execv

class raw_metadata():
    def __init__(self,
                 name="",
                 extention="json",
                 schema="raw",
                 action="download from NYC 311 API",
                 creator="",
                 machine="",
                 localhost = "",
                 ip="",
                 creation_date="",
                 size="",
                 location="",
                 status="sucess",
                 param_year="",
                 param_month="",
                 param_day="",
                 param_bucket=""):

        # asignamos las características de los metadatos
        self.name = name
        self.extention = extention
        self.schema = schema
        self.action = action
        self.creator = creator
        self.machine = machine
        self.ip = ip
        self.creation_date = creation_date
        self.size = size
        self.location = location
        self.status= status
        self.param_year = param_year
        self.param_month = param_month
        self.param_day = param_day
        self.param_bucket = param_bucket

    def lista(self):
        return (self.name, self.extention, self.schema, self.action,
               self.creator, self.machine, self.ip, self.creation_date,
               self.size, self.location, self.status, self.param_year,
               self.param_month, self.param_day, self.param_bucket)

def metaDataRaw():
            cwd = os.getcwd() # directorio actual
            raw_meta = raw_metadata()
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

def queryApi311(year, month, day):
    # Usado en Task1 : Consulta a la API
    # Autenticación del cliente:
    client = Socrata(settings.get('dburl'),
                     settings.get('apptoken'),
                     username=settings.get('user'),
                     password=settings.get('pass'))

    # los resultados son retornados como un archivo JSON desde la API /
    # convertida a una lista de Python usando sodapy
    client.timeout = 1000
    limit = 1000000000

    # query
    results = client.get(
        "erm2-nwe9", limit=limit, where=f"created_date between '{year}-{month}-{day}T00:00:00.000' and '{year}-{month}-{day}T23:59:59.999'")

    return results

def execv(command, path):
    '''
    Esta función devuelve el stdout y stderr de un comando de bash ejecutado en
    python.
        parámetros:
        * command (str): comando en bash
        * path (str): directorio donde se ejecuta el comando

    Referencia:
    https://stackoverflow.com/questions/11048360/how-to-execute-shell-command-get-the-output-and-pwd-after-the-command-in-python
    '''
    command = 'cd %s && %s && pwd 1>&2' % (path, command)
    proc = sub.Popen(['/bin/bash', '-c', command],
                     stdout=sub.PIPE, stderr=sub.PIPE)
    stderr = proc.stderr.read()[:-1]
    stdout = proc.stdout.read()[:-1]
    if stdout == '' and not os.path.exists(stderr):
        raise Exception(stderr)
    return stdout.decode('utf-8') #, stderr.decode('utf-8')


def get_extract_metadata(file_path,cwd):
    # obterner solo el nombre del archivo
    cmd_name = "echo %s | awk -F \"/\" \'{print $NF}\'" % (file_path)
    file_name = execv(cmd_name, cwd)
    name_cmd = "echo  %s | awk -F\".\" \'{print $1}\'" % (file_name)
    ext_cmd = "ls -lad %s | awk -F\".\" \'{print $NF}\' " % (file_path)
    cre_cmd = "ls -lad %s | awk \'{print $3}\'" % (file_path)
    mch_cmd = "  uname -a"
    ip_cmd = "curl ipecho.net/plain ; echo"
    cdt_cmd = "ls -lad %s | awk \'{print $6\"-\"$7\"-\"$8}'" % (file_path)
    siz_cmd = "ls -lad -h %s | awk \'{print $5}\'" % (file_path)
    ent_cmd = "jq length %s" % (file_path)

    # llena el df
    file_name = execv(cmd_name, cwd)
    name = execv(name_cmd, cwd)
    extention = execv(ext_cmd, cwd)
    schema = 'raw'
    action = 'download'
    creator = execv(cre_cmd, cwd)
    machine = execv(mch_cmd, cwd)
    ip = execv(ip_cmd, cwd)
    creation_date = execv(cdt_cmd, cwd)
    size = execv(siz_cmd, cwd)
    location = file_path
    entries = execv(ent_cmd, cwd)
    variables = 'a'
    script = 'a'
    log_script = 'a'
    status = 'a'

    return (str(name),
            str(extention),
            str(schema),
            str(action),
            str(creator),
            str(machine),
            str(ip),
            str(creation_date),
            str(size),
            str(location),
            str(entries),
            variables,
            script,
            log_script,
            status)
