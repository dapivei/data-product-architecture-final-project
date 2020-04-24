<div class="tip" markdown="1">

# Configuración AWS
<div align="justify">

Esta carpeta contiene los *scripts* e indicaciones necesarias para la configuración del bastión y la máquina donde se llevará acabo el procesamiento.

### I. Configuración bastión

El bastión es una instancia pequeña `t2.micro`  con `8gb` de memoria (no es necesario tener algo con mayor capacidad en este punto). Funciona como punto de seguridad y administra a los usuarios para que puedan entrar a la arquitectura de AWS.

Este `ec2` vive dentro de una subnet pública en una VPC.

Para tener la conexión al bastión, ocupamos el protocolo `ssh` como sigue:

```
ssh -o "ServerAliveInterval 60" -i /ruta/llave.pem  ubuntu@ip-ec2
```

Para salir, basta con poner `exit` en el shell de la instancia.


### II. addus.sh

Este script crea los usuarios dentro del bastión y asigna la misma contraseña a cada uno de ellos, por lo que debemos estar conectados a la instancia. Para poder hacer uso es necesario darle los permisos, es decir, ` chmod +x addus.sh`. También es importante mencionar que se utiliza un archivo txt `users.txt` que contiene el nombre de los usuarios que queremos agregar.

Para la selección de contraseña, utilizamos el encriptador `crypt` contenido en python 3.6.5 usando los siguientes comandos:

```
import crypt
crypt.crypt("contraseña","salt")
```

La salida de estos comandos debe verse como: ` 'sa1O7Z1pCJzK.' `; esta debe agregarse al script `addus.ssh` en la parte indicada. Para correr el archivo solo es necesario correr el siguiente comando:

 ```
 ./addus.sh
 ```
En este punto ya tenemos a los usuarios agregados: todos con la misma contraseña y permisos de super usuario.

### III.  Agregar llaves para conexión

Una vez creados los usuarios, copiaremos las llaves públicas de cada uno de los usuarios a su usuario correspondiente.

1° Es necesario modificar el archivo `sudo nano /etc/ssh/sshd_config` y cambiar los parámetros a la siguiente configuración:

```
PubkeyAuthentication yes
PubkeyAuthentication yes
```

Ahora solo es necesario reiniciar el servicio sshd con el comando:

```
sudo service sshd restart

```

2° Realizar el copiado de las llaves al bastión, de forma segura:

```
ssh-copy-id -f -i /ruta/llave/id_llave.pub usuario@ip-ec2
```

3° Finalmente, nos reconectamos al bastión y cambiamos la configuración del archivo `sudo nano /etc/ssh/sshd_config` dejando el siguiente parámetro:

```
PasswordAuthentication no
```

4° Reiniciamos el servicio `sudo service sshd restart `

Para conectarnos usando el protocolo `ssh`  y la llave privada, utilizamos el siguiente comando:

```
ssh -o "ServerAliveInterval 60" -i /ruta/llave_privada  usuario@ip-ec2
```

### IV. EC2 procesamiento

Esta máquina es para el procesamiento de la información y es donde viven las tareas que realiza `luigi`; cabe mencionar que esta instancia es de mayor capacidad. Aquí también debe ser instalado `docker` para la reproducibilidad del proyecto.

Esta maquina vive dentro de una VPC con una subnet privada. Para tener acceso a la misma tenemos que hacer un secure copy de la llave.pem al bastion:

```
ssh -i  /tu/ruta/llave-bastion.pem /tu/ruta/llave/ec2/llave.pem ubuntu@ip-ec2:/home/ubuntu/.ssh
```

Nos conectamos al bastión y en la ruta `/home/ubuntu/.ssh` tendremos guardada la `llave.pem`. En este punto solo tendremos que hacer la conexión usando el protocolo `ssh`.


### Para levantar cluster

A continuación se presentan los pasos a seguir para configurar y levantar el cluster, conexión y trabajo dentro del mismo. Para el óptimo desarrollo se tiene como consideración que los datos se encuentran guardados en una `S3`.

Conectado en el servicio `EMR` de `AWS`, damos click en el botón `crear cluster`. Nombramos el cluster; en configuración de software escogemos `emr-5.29.0` con aplicaciones `Spark: Spark 2.4.4 on Hadoop 2.8.5 YARN with Ganglia 3.7.2 and Zeppelin 0.8.2` y finalmente seleccionamos `crear cluster`

Para la conexión al cluster es recomendable utilizar el explorador `chrome`. En el explorador abrimos la siguiente (liga)[https://chrome.google.com/webstore/detail/foxyproxy-standard/gcknhkkoolaabfmlnjonogaaifnjlfnp?hl=es] para instalar el complemento `FoxyProxy Standard`, dentro de este complemento en `options > import/export ` subimos el archivo `foxyproxy-settings.xml`, por último en `proxy mode` seleccionamos la opción `Use proxies based on their pre-defined patterns and priorities`

+ 1. Abrimos el tunel `ssh` desde la linea de comandos de la siguiente forma.
```
ssh -i ~/ruta/llave.pem -ND 8157 hadoop@dns_ip_aws
```
+ 2. Desde `Choreme` nos conectamos a la siguiente ruta para utilizar `Zeppelin`.
```
dns_ip_aws:8890
```



</div>
