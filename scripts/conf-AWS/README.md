<div class="tip" markdown="1">

# Configurción AWS
<div align="justify">

Esta carpeta tiene los scripts e indicaciones necesarias para la configuración del bastión y la máquina donde se lleve acabo el procesamiento.

### I Configuración bastión


### II addus.sh

Esté script crea los usuarios dentro del bastión y asigna la misma contraseña a cada uno de ellos; para poder hacer uso es necesario darle los permisos, es decir, ` chmod +x addus.sh`. También es importante mencionar que se utiliza un archivo txt `users.txt` que contiene el nombre de los usuarios que queremos agregar.

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

### III  Agregar llaves para conexión

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

### IV EC2 procesamiento

Esta máquina es para el procesamiento de la información y es donde viven las tareas que realiza `luigi`, cabe mencionar que está instancia es de mayor capacidad. Aquí también debe ser instalado docker para la reproducibilidad del proyecto.
</div>
