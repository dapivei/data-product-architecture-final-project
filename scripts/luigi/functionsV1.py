import subprocess as sub

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
    '''
    Esta funcion genera los metadatos para la tarea de extraccion de datos de la API.
    '''

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

def get_preproc_metadata(file_path,cwd,file):
    '''
    Esta funcion genera los metadatos para la tarea de cambiar de json a parquet.
    '''
    # introducir nombreext_cmd = "ls -lad %s | awk -F\".\" \'{print $NF}\' " % (file)
    cmd_name = "echo %s | awk -F \"/\" \'{print $NF}\'" % (file)
    #cmd_name= execv(cmd_name, cwd)
    ext_cmd = "ls -lad %s | awk -F\".\" \'{print $NF}\' " % (file)
    #df.at[count, 'extention'] = functions.execv(ext_cmd, cwd)
    # esquema y acción
    #df.at[count, 'schema'] = 'preprocessing'
    #df.at[count, 'action'] = 'transform json to parquet'
    # otras características de la creación
    cre_cmd = "ls -lad %s | awk \'{print $3}\'" % (file)
    #df.at[count, 'creator'] = functions.execv(cre_cmd, cwd)
    mch_cmd = "uname -a"
    #df.at[count, 'machine'] = functions.execv(mch_cmd, cwd)
    ip_cmd = "curl ipecho.net/plain ; echo"
    #df.at[count, 'ip'] = functions.execv(ip_cmd, cwd)
    cdt_cmd = "ls -lad %s | awk \'{print $6\"-\"$7\"-\"$8}\'" % (file)
    #df.at[count, 'creation_date'] = functions.execv(cdt_cmd, cwd)
    siz_cmd = "ls -lad -h %s | awk \'{print $5}\'" % (file)
    #df.at[count, 'size'] = functions.execv(siz_cmd, cwd)
    #df.at[count, 'location'] = file_path

    # llena el df
    name = execv(cmd_name, cwd)
    #name = execv(name_cmd, cwd)
    extention = execv(ext_cmd, cwd)
    schema = 'preprocessing'
    action = 'transform'
    creator = execv(cre_cmd, cwd)
    machine = execv(mch_cmd, cwd)
    ip = execv(ip_cmd, cwd)
    creation_date = execv(cdt_cmd, cwd)
    size = execv(siz_cmd, cwd)
    location = file_path
    #entries = execv(ent_cmd, cwd)
    entries ='a'
    variables = execv("ls | wc -l", file_path)
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
            str(variables),
            str(script),
            str(log_script),
            str(status))
