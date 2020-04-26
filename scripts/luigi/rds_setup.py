#!/usr/bin/env python
# -*- coding: utf-8 -*-

import psycopg2 as ps
from dynaconf import settings

#connect
conn=ps.connect(host=settings.get('host'),
                port=settings.get('port'),
                database=settings.get('database'),
                user=settings.get('usr'),
                password=settings.get('password'))
cur = conn.cursor()

#read file
f=open('sql/create_raw_schema.sql','r')
sql=str(f.read())
cur.execute(sql)
conn.commit()

f=open('sql/create_preprocessed_schema.sql','r')
sql=str(f.read())
cur.execute(sql)
conn.commit()

f=open('sql/create_cleaned_schema.sql','r')
sql=str(f.read())
cur.execute(sql)
conn.commit()

cur.close()
conn.close()
