import subprocess as sub
import pandas as pd

def create_feature_table(df,h=10):
    import numpy as np
    '''
    Esta fucnion crea la tabla que contiene los resumenes de llamadas diarias.
    Sigue el proceso:
        Lee los registros diarios
        Agrega columnas de interes
        Resume los registros
    Devuleve un registro del dia que estamos analizando
    '''

    # Created date (Obtenemos año, mes y dia)
    df['created_date_year'] = pd.DatetimeIndex(df['created_date']).year
    df['created_date_month'] = pd.DatetimeIndex(df['created_date']).month
    df['created_date_day'] = pd.DatetimeIndex(df['created_date']).day

    # Creamos la variable created_date_dow (numero del dia de la semana de la fecha created_date)
    df['created_date_dow'] = pd.DatetimeIndex(df['created_date']).dayofweek

    # Creamos la variable created_date_woy (numero de la semana del año de la fecha created_date)
    df['created_date_woy'] = pd.DatetimeIndex(df['created_date']).week

    # Creamos variable holiday
    df["date_holiday"]=df["created_date"].apply(festivo)

    #dias de historia
    distrito=df['borough'].unique()
    flag=0
    for dist in distrito:
        df2 = df[df['borough']==dist].reset_index(drop=True)
        for h in range(1,10):
            var_name = "number_cases_" + str(h) + "_days_ago"
            a=df2['counts']
            aa = pd.DataFrame(np.zeros(h))
            b=pd.concat([aa,a[:-h]]).reset_index(drop=True)
            b.columns=[var_name]
            df2=pd.concat([df2,b],axis=1).reset_index(drop=True)

        if flag==0:
            df3=df2
            flag=1
        else:
            df3=pd.concat([df3,df2]).reset_index(drop=True)
        del(df2)
    #variable respuesta
    df=df3
    del(df3)
    means=df.loc[:,['created_date_month','counts','borough']]
    means=means.groupby(['created_date_month','borough'],as_index=False).mean()
    print(means)
    means.columns=['created_date_month','borough','month_mean']
    df=pd.merge(df,means,how='left',on=['created_date_month','borough'])
    print(df)
    #flag de arriba o abajo del promedio
    df["mean_flag"]= df["counts"] - df["month_mean"]
    df["mean_flag"]=df["mean_flag"].apply(flags)
    return df

def encoders(df):
    import pandas as pd
    from sklearn.preprocessing import LabelEncoder
    le_created_date_year = LabelEncoder()
    le_created_date_month = LabelEncoder()
    le_created_date_day = LabelEncoder()
    le_created_date_dow = LabelEncoder()
    le_created_date_woy = LabelEncoder()
    df['created_date_year_encoded'] = le_created_date_year.fit_transform(df.created_date_year)
    df['created_date_month_encoded'] = le_created_date_month.fit_transform(df.created_date_month)
    df['created_date_day_encoded'] = le_created_date_day.fit_transform(df.created_date_day)
    df['created_date_dow_encoded'] = le_created_date_dow.fit_transform(df.created_date_dow)
    df['created_date_woy_encoded'] = le_created_date_woy.fit_transform(df.created_date_woy)

    from sklearn.preprocessing import OneHotEncoder
    created_date_year_ohe = OneHotEncoder()
    created_date_month_ohe = OneHotEncoder()
    created_date_day_ohe = OneHotEncoder()
    created_date_dow_ohe = OneHotEncoder()
    created_date_woy_ohe = OneHotEncoder()
    X = created_date_year_ohe.fit_transform(df.created_date_year_encoded.values.reshape(-1,1)).toarray()
    Xm = created_date_month_ohe.fit_transform(df.created_date_month_encoded.values.reshape(-1,1)).toarray()
    Xo = created_date_day_ohe.fit_transform(df.created_date_day_encoded.values.reshape(-1,1)).toarray()
    Xt = created_date_dow_ohe.fit_transform(df.created_date_dow_encoded.values.reshape(-1,1)).toarray()
    Xz = created_date_woy_ohe.fit_transform(df.created_date_woy_encoded.values.reshape(-1,1)).toarray()

    dfOneHot = pd.DataFrame(X, columns = ["created_date_year_"+str(int(i)) for i in range(X.shape[1])])
    df = pd.concat([df, dfOneHot], axis=1)
    dfOneHot = pd.DataFrame(Xm, columns = ["created_date_month_"+str(int(i)) for i in range(Xm.shape[1])])
    df = pd.concat([df, dfOneHot], axis=1)
    dfOneHot = pd.DataFrame(Xo, columns = ["created_date_day_"+str(int(i)) for i in range(Xo.shape[1])])
    df = pd.concat([df, dfOneHot], axis=1)
    dfOneHot = pd.DataFrame(Xt, columns = ["created_date_dow_"+str(int(i)) for i in range(Xt.shape[1])])
    df = pd.concat([df, dfOneHot], axis=1)
    dfOneHot = pd.DataFrame(Xz, columns = ["created_date_woy_"+str(int(i)) for i in range(Xz.shape[1])])
    df = pd.concat([df, dfOneHot], axis=1)

    return df


def flags(x):
    if(x<0):
        return 0
    else:
        return 1

def festivo(date):
    '''
    Recorre el arreglo de dias festivos e indica si un dia es festivos
    Funcion auxiliar a create_feature_table
    '''
    import datetime
    h = ['2010-01-01', '2010-12-31', '2010-01-18', '2010-02-15', '2010-05-31', '2010-07-04', '2010-07-05', '2010-09-06', '2010-10-11', '2010-11-11', '2010-11-25', '2010-12-25', '2010-12-24', '2011-01-01', '2010-12-31', '2011-01-17', '2011-02-21', '2011-05-30', '2011-07-04', '2011-09-05', '2011-10-10', '2011-11-11', '2011-11-24', '2011-12-25', '2011-12-26', '2012-01-01', '2012-01-02', '2012-01-16', '2012-02-20', '2012-05-28', '2012-07-04', '2012-09-03', '2012-10-08', '2012-11-11', '2012-11-12', '2012-11-22', '2012-12-25', '2013-01-01', '2013-01-21', '2013-02-18', '2013-05-27', '2013-07-04', '2013-09-02', '2013-10-14', '2013-11-11', '2013-11-28', '2013-12-25', '2014-01-01', '2014-01-20', '2014-02-17', '2014-05-26', '2014-07-04', '2014-09-01', '2014-10-13', '2014-11-11', '2014-11-27', '2014-12-25', '2015-01-01', '2015-01-19', '2015-02-16', '2015-05-25', '2015-07-04', '2015-07-03', '2015-09-07', '2015-10-12', '2015-11-11', '2015-11-26', '2015-12-25', '2016-01-01', '2016-01-18', '2016-02-15', '2016-05-30', '2016-07-04', '2016-09-05', '2016-10-10', '2016-11-11', '2016-11-24', '2016-12-25', '2016-12-26', '2017-01-01', '2017-01-02', '2017-01-16', '2017-02-20', '2017-05-29', '2017-07-04', '2017-09-04', '2017-10-09', '2017-11-11', '2017-11-10', '2017-11-23', '2017-12-25', '2018-01-01', '2018-01-15', '2018-02-19', '2018-05-28', '2018-07-04', '2018-09-03', '2018-10-08', '2018-11-11', '2018-11-12', '2018-11-22', '2018-12-25', '2019-01-01', '2019-01-21', '2019-02-18', '2019-05-27', '2019-07-04', '2019-09-02', '2019-10-14', '2019-11-11', '2019-11-28', '2019-12-25', '2020-01-01', '2020-01-20', '2020-02-17', '2020-05-25', '2020-07-04', '2020-07-03', '2020-09-07', '2020-10-12', '2020-11-11', '2020-11-26', '2020-12-25']
    h=pd.to_datetime(h).date
    for festive in h:
        if(date==festive):
            return 1
    return 0

def to_cleaned(df):
    #estandariza notacion de nulos
    df=df.replace(['N/A', 'nan', 'NaN', 'n/a', 'Na', ''],None)

    #pasa todo a minusculas
    cols=df.columns
    for col in cols:
        try:
            df[col]=df[col].str.lower()
        except:
            next

    #crea nuevas columnas
    df["created_date_timestamp"]=pd.to_datetime(df["created_date"])
    df["closed_date_timestamp"]=pd.to_datetime(df["closed_date"])
    df["due_date_timestamp"]=pd.to_datetime(df["due_date"])
    df["resolution_action_updated_date_timestamp"]=pd.to_datetime(df["resolution_action_updated_date"])

    #asigna formato de fechas
    timeFmt="yyyy-MM-dd'T'HH:mm:ss.SSS"
    df["created_date"]=df["created_date_timestamp"].dt.date
    df["closed_date"]=df["closed_date_timestamp"].dt.date
    df["due_date"]=df["due_date_timestamp"].dt.date
    df["resolution_action_updated_date"]=df["resolution_action_updated_date_timestamp"].dt.date

    #quita duplicados y columnas vacias
    df=df.drop_duplicates()
    df=df.dropna(axis=1)
    return df


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
