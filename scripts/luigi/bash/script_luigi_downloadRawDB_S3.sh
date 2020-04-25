#!/bin/bash

# Script for preprocessing data from 2010/1 to 2020/3 using Parameters
# year, month, and day in downloadRawJSONData luigi task.

for year in {2010..2019}
do
  for month in {1..12}
  do
    for day in {1..31}
    do
    PYTHONPATH="." PYTHONPATH="." AWS_PROFILE=luigi_dpa  luigi --module luigi_orchestration_ec2 Task_20_metaDownload --local-scheduler --year $year --month $month --day $day
    sleep 0.5s
    done
  done
done

# para 2020
for month in {1..3}
  do
    for day in {1..31}
      do
    PYTHONPATH="." PYTHONPATH="." AWS_PROFILE=luigi_dpa  luigi --module luigi_orchestration_ec2 Task_20_metaDownload --local-scheduler --year 2020 --month $month --day $day
    sleep 0.5s
    
    done
done

