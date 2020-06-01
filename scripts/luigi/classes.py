#!/usr/bin/env python
# -*- coding: utf-8 -*-

import json
import boto3
import botocore
import datetime
import getpass
import luigi
import luigi.contrib.s3

import os
import pandas as pd
import platform
import psycopg2 as ps
import pyarrow as pa
import socket
import numpy as np
import pickle
import s3fs

from luigi.contrib.postgres import CopyToTable
from functionsV2 import queryApi311
from functionsV2 import execv

from datetime import date
from dynaconf import settings
from luigi.contrib.s3 import S3Client, S3Target
from sodapy import Socrata

# ===================== Clases para guardar metadatos  =========================
# Se definen dos clases que guarden las características de los metadatos
# Nota: A este momento las dos clases son idénticas. Se van a realizar ajustes
# para que tengan características específicas:
# raw_metadata
# - corregir status de tarea de luigi
# - Size: tamaño de object en bucket de S3.
# preproc_metadata
# - Archivo de origen: (key de S3 object)
# - Size: tamaño de object en bucket de S3.
# - corregir status de tarea de luigi
# ==============================================================================

# ===================== Clases para guardar metadatos  =========================
# Se definen dos clases que guarden las características de los metadatos
# Nota: A este momento las dos clases son idénticas. Se van a realizar ajustes
# para que tengan características específicas:
# raw_metadata
# - corregir status de tarea de luigi
# - Size: tamaño de object en bucket de S3.
# preproc_metadata
# - Archivo de origen: (key de S3 object)
# - Size: tamaño de object en bucket de S3.
# - corregir status de tarea de luigi
# ==============================================================================

# ========== schema raw  ==========


class raw_metadata():
    def __init__(self,
                 name="",
                 extention="json",
                 schema="raw",
                 action="download from NYC 311 API",
                 creator="-",
                 machine="",
                 localhost="",
                 ip="",
                 creation_date="",
                 size="-",
                 location="",
                 status="sucess",
                 param_year="",
                 param_month="",
                 param_day="",
                 param_bucket=""):

        # asignamos las características de los metadatos
        self.name = name
        self.extention = extention
        self.schema = schema
        self.action = action
        self.creator = creator
        self.machine = machine
        self.ip = ip
        self.creation_date = creation_date
        self.size = size
        self.location = location
        self.status = status
        self.param_year = param_year
        self.param_month = param_month
        self.param_day = param_day
        self.param_bucket = param_bucket

    def info(self):
        return (self.name, self.extention, self.schema, self.action,
                self.creator, self.machine, self.ip, self.creation_date,
                self.size, self.location, self.status, self.param_year,
                self.param_month, self.param_day, self.param_bucket)

# ========== schema preprocess  ==========


class preproc_metadata():
    def __init__(self,
                 name="",
                 extention="parquet",
                 schema="preprocess",
                 action="transform JSON to parquet",
                 creator="-",
                 machine="",
                 localhost="",
                 ip="",
                 creation_date="",
                 size="-",
                 location="",
                 status="sucess",
                 param_year="",
                 param_month="",
                 param_day="",
                 param_bucket=""):

        # asignamos las características de los metadatos
        self.name = name
        self.extention = extention
        self.schema = schema
        self.action = action
        self.creator = creator
        self.machine = machine
        self.ip = ip
        self.creation_date = creation_date
        self.size = size
        self.location = location
        self.status = status
        self.param_year = param_year
        self.param_month = param_month
        self.param_day = param_day
        self.param_bucket = param_bucket

    def info(self):
        return ([self.name, self.extention ,self.schema, self.action,
                self.creator,self.machine,self.ip, self.creation_date,
                self.size, self.location, self.status,self.param_year,
                self.param_month, self.param_day, self.param_bucket])

###############################################################################

class cleaned_metadata():
    def __init__(self,
                 name="",
                 extention="parquet",
                 schema="cleaned",
                 action="clean parquet",
                 creator="-",
                 machine="",
                 localhost="",
                 ip="",
                 creation_date="",
                 size="-",
                 location="",
                 status="sucess",
                 param_year="",
                 param_month="",
                 param_day="",
                 param_bucket=""):

        # asignamos las características de los metadatos
        self.name = name
        self.extention = extention
        self.schema = schema
        self.action = action
        self.creator = creator
        self.machine = machine
        self.ip = ip
        self.creation_date = creation_date
        self.size = size
        self.location = location
        self.status = status
        self.param_year = param_year
        self.param_month = param_month
        self.param_day = param_day
        self.param_bucket = param_bucket

    def info(self):
        return (self.name, self.extention, self.schema, self.action,
                self.creator, self.machine, self.ip, self.creation_date,
                self.size, self.location, self.status, self.param_year,
                self.param_month, self.param_day, self.param_bucket)


# ========= metadatos unit test de cleaned =========#
class cleaned_metadataUnitTest():
    def __init__(self,
                 name="",
                 extention="parquet",
                 schema="cleaned",
                 action="unit test for clenead: test_for_closed_date_greater_than_created_date & test_for_years_out_of_range",
                 creator="-",
                 machine="",
                 localhost="",
                 ip="",
                 creation_date="",
                 size="-",
                 location="",
                 status="OK",
                 param_year="",
                 param_month="",
                 param_day="",
                 param_bucket=""):

        # asignamos las características de los metadatos
        self.name = name
        self.extention = extention
        self.schema = schema
        self.action = action
        self.creator = creator
        self.machine = machine
        self.ip = ip
        self.creation_date = creation_date
        self.size = size
        self.location = location
        self.status = status
        self.param_year = param_year
        self.param_month = param_month
        self.param_day = param_day
        self.param_bucket = param_bucket

    def info(self):
        return (self.name, self.extention, self.schema, self.action,
                self.creator, self.machine, self.ip, self.creation_date,
                self.size, self.location, self.status, self.param_year,
                self.param_month, self.param_day, self.param_bucket)

###############################################################################


# ========= metadatos unit test de cleaned =========#
class FE_metadataUnitTest():
    def __init__(self,
                 name="",
                 extention="parquet",
                 schema="cleaned",
                 action="",
                 creator="-",
                 machine="",
                 localhost="",
                 ip="",
                 creation_date="",
                 size="-",
                 location="",
                 status="OK",
                 param_bucket=""):

        # asignamos las características de los metadatos
        self.name = name
        self.extention = extention
        self.schema = schema
        self.action = action
        self.creator = creator
        self.machine = machine
        self.ip = ip
        self.creation_date = creation_date
        self.size = size
        self.location = location
        self.status = status
        self.param_bucket = param_bucket

    def info(self):
        return (self.name, self.extention, self.schema, self.action,
                self.creator, self.machine, self.ip, self.creation_date,
                self.size, self.location, self.status, self.param_bucket)


###################################################################
# clase y tarea de guardado de metadatos de modelado
class model_metadata():
    def __init__(self,
                 model_name="",
                 model_type="sklearn model",
                 schema="modelling",
                 action="ML training model",
                 creator="-",
                 machine="",
                 ip="",
                 date="",
                 location="",
                 status="sucess",
                 max_depth="",
                 criterion="",
                 n_estimators="",
                 score_train="",
                 param_year="",
                 param_month="",
                 param_day="",
                 param_bucket="",
                 ):

        # asignamos las características de los metadatos
        self.model_name = model_name
        self.model_type = model_type
        self.schema = schema
        self.action = action
        self.creator = creator
        self.machine = machine
        self.ip = ip
        self.date = date
        self.location = location
        self.status = status
        self.max_depth = max_depth
        self.criterion = criterion
        self.n_estimators = n_estimators
        self.score_train = score_train
        self.param_year = param_year
        self.param_month = param_month
        self.param_day = param_day
        self.param_bucket = param_bucket
        
    def info(self):
        return (self.model_name, self.model_type, self.schema, self.action,
                self.creator, self.machine, self.ip, self.date, self.location,
                self.status, self.max_depth, self.criterion, self.n_estimators,
                self.score_train,self.param_year,self.param_day,self.param_month,
                self.param_bucket)