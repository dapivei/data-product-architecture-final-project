<div class="tip" markdown="1">

# Configurción AWS
<div align="justify">

Esta carpeta tiene los scripts e indicaciones necesarias para la configuración del bastión y la máquina donde se llevará acabo el procesamiento.

### I. Configuración bastión

El bastión es una instancia pequeña `t2.micro`  con `8gb` de memoria (no es necesario tener algo con mayor capacidad en este punto). Funciona como punto de seguridad y administra a los usuarios para que puedan entrar a la arquitectura de AWS.

Está ec2 vive dentro de una subnet pública en una VPC.

Para tener la conexión al bastión ocupamos el protocolo `ssh` como sigue:
```
ssh -o "ServerAliveInterval 60" -i /ruta/llave.pem  ubuntu@ip-ec2
```
y para salir basta con poner `exit` en el shell de la instancia

### II. addus.sh

Esté script crea los usuarios dentro del bastión y asigna la misma contraseña a cada uno de ellos, por lo que debemos estar conectados a la instancia. Para poder hacer uso es necesario darle los permisos, es decir, ` chmod +x addus.sh`. También es importante mencionar que se utiliza un archivo txt `users.txt` que contiene el nombre de los usuarios que queremos agregar.

Para la selección de contraseña, utilizamos el encriptador `crypt` contenido en python 3.6.5 usando los siguientes comandos:

```
import crypt
crypt.crypt("contraseña","salt")
```
La salida de estos comandos debe verse como: ` 'sa1O7Z1pCJzK.' ` está se debe agregar al script addus.ssh en la parte indicada. Para correr el archivo solo es necesario corer el siguiente comando:
 ```
 ./addus.sh
 ```
En este punto ya tenemos los usuarios agregados, todos con la misma contraseña y permisos de super usuario.

### III.  Agregar llaves para conexión

Una vez creados los usuarios copiaremos las llaves públicas de casa uno de los usuarios a su usuario correspondiente.

1° Es necesario modificar el archivo `sudo nano /etc/ssh/sshd_config` y cambiar los parámetros a la siguiente congiguración:

```
PubkeyAuthentication yes
PubkeyAuthentication yes
```
Ahora solo es necesario reiniciar el servicio sshd con el comando `sudo service sshd restart `

2° Hacer de forma segura el copiado de las llaves al bastión

```
ssh-copy-id -f -i /ruta/llave/id_llave.pub usuario@ip-ec2
```

3° Finalmente, nos volvemos a conectar al bastión y cambiamos la configuración del archivo `sudo nano /etc/ssh/sshd_config` dejando el siguiente parámetro:
```
PasswordAuthentication no
```

Y hacemos el reinicio del servicio `sudo service sshd restart `

Para conectarnos usando el protocolo `ssh`  y la llave privada, utilizamos el siguiente comando
```
ssh -o "ServerAliveInterval 60" -i /ruta/llave_privada  usuario@ip-ec2
```

### IV. EC2 procesamiento

Esta máquina es para el procesamiento de la información y es donde viven las tareas que realiza `luigi`, cabe mencionar que está instancia es de mayor capacidad. Aquí también debe ser instalado `docker` para la reproducibilidad del proyecto.

Esta maquina vive desntro de una VPC con una subnet privada, para tener acceso a la misma tenemos que hacer un secure copy de la llave.pem al bastion.

```
ssh -i  /tu/ruta/llave-bastion.pem /tu/ruta/llave/ec2/llave.pem ubuntu@ip-ec2:/home/ubuntu/.ssh
```

Nos conectamos al bastión y en la ruta `/home/ubuntu/.ssh` tendremos guardada la `llave.pem`. En este punto solo tendremos que hacer la conexión usando el protocolo `ssh`.

</div>
