<div class="tip" markdown="1">

# Configurción AWS
<div align="justify">

Esta carpeta tiene los scripts e indicaciones necesarias para la configuración del bastión y la máquina donde se lleve acabo el procesamiento.

### I addus.ssh

Esté script crea los usuarios dentro del bastion y asigna la misma contraseña a cada uno de ellos; para poder hacer uso es necesario darle los permisos, es decir, ` chmod +x addus.ssh`. También es importante mencionar que se utiliza un archivo txt `users.txt` que contiene el nombre de los usuarios que queremos agregar.

Para la selección de contraseña, utilizamos el encriptador `crypt` contenido en python 3.6.5 usando los siguientes comandos:

``
import crypt
crypt.crypt("contraseña","salt")
``




</div>
