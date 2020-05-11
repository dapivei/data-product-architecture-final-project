#!/bin/bash

day=${1}
month=${2}
year=${3}
bucket=${4}
#imprime STDERR en pantalla pero no imprime info en archivo
P_DAY=$day P_MONTH=$month P_YEAR=$year P_BUCKET=$bucket python3 -m marbles unit_test/test_cleaned.py

#asi redireige STDERR a un archivo pero no imprime en pantalla
#P_DAY=$day P_MONTH=$month P_YEAR=$year P_BUCKET=$bucket python3 -m marbles test_cleaned.py &> clean_test_output.txt

#python3 -m marbles test_cleaned.py 2>&1 | tee cleaned_output.txt

#NOTA: si redirigo el STDERR al archivo no imprime nada de la pruba en el output de luigi
# y si llamo cat para imprimir ya no truena nunca (por que se sobreescribe el ultimo STDERR)
# entonces hay que buscar como darle la vuelta a eso o no escribir info sobre la prueba en matadata

#esta sentencia hace que siempre termine bien e imprime prueba en pantalla
#cat a.txt
##cat a.txt | grep Ran > a.txt
