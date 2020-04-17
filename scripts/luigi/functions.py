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
