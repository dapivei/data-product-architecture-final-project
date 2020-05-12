#!/bin/bash

bucket=${1}

#asi redireige STDERR a un archivo e imprime en pantalla
P_BUCKET=$bucket python3 -m marbles unit_test/test_feature.py 2>&1 | tee feature_test_output.txt

#si encuentra FAIL regresa estatus fallido.
#De otro modo regresa estatus completo
! cat feature_test_output.txt | grep FAIL
