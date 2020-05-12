#!/bin/bash

day=${1}
month=${2}
year=${3}
bucket=${4}

#asi redireige STDERR a un archivo e imprime en pantalla
P_DAY=$day P_MONTH=$month P_YEAR=$year P_BUCKET=$bucket python3 -m marbles unit_test/test_cleaned.py 2>&1 | tee clean_test_output.txt

#si encuentra FAIL regresa estatus fallido.
#De otro modo regresa estatus completo
! cat clean_test_output.txt | grep FAIL
