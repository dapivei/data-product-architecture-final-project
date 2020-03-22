
#!/bin/bash
# Script para agregar usuarios a ubuntu 
# El password y permisos para todos los usuarios es el mismo
# En la variable P se debe agregar la contraseña encriptada, leer readme.
P='Agregar-contraseña'
echo $P
for i in $( cat users.txt ); do
    adduser $i
    echo "user $i added successfully!"
    usermod -p $P $i
    echo "Password for user $i changed successfully"
    usermod -aG sudo $i
done
