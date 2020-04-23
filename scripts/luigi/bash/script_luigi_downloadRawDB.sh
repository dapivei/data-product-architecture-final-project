#!/bin/bash

# Script for downloading data from 2010 to 2020 using Parameters
# year, month, and day in downloadRawJSONData luigi task.

for year in {2010..2019}
do
  for month in {1..12}
  do
    for day in {1..31}
    do
    PYTHONPATH="." luigi --module luigi_local_downloadRaw downloadRawJSONData --local-scheduler --year $year --month $month --day $day
    sleep 2s
    done
  done
done
