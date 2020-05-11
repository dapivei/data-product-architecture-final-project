#!/bin/bash

day=${1}
month=${2}
year=${3}
bucket=${4}
P_DAY=$day P_MONTH=$month P_YEAR=$year P_BUCKET=$bucket python3 -m marbles test_cleaned.py &> a.txt
