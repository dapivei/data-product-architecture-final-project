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
    variables = None
    script = None
    log_script = None
    status = None

    return (name,
            extention,
            schema,
            action,
            creator,
            machine,
            ip,
            creation_date,
            size,
            location,
            entries,
            variables,
            script,
            log_script,status)
