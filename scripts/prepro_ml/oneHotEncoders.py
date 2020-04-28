#!/usr/bin/env python

#________________________________________________________________________________________

CREATING ONE HOT ENCODERS

#----------------------------------------------------------------------------------------
import pandas as pd
from sklearn.preprocessing import LabelEncoder
le_created_date_year = LabelEncoder()
le_created_date_month = LabelEncoder()
le_created_date_day = LabelEncoder()
le_created_date_dow = LabelEncoder()
le_created_date_woy = LabelEncoder()
df['created_date_year_encoded'] = le_created_date_year.fit_transform(df.created_date_year)
df['created_date_month_encoded'] = le_created_date_month.fit_transform(df.created_date_month)
df['created_date_day_encoded'] = le_created_date_day.fit_transform(df.created_date_day)
df['created_date_dow_encoded'] = le_created_date_dow.fit_transform(df.created_date_dow)
df['created_date_woy_encoded'] = le_created_date_woy.fit_transform(df.created_date_woy)

from sklearn.preprocessing import OneHotEncoder
created_date_year_ohe = OneHotEncoder()
created_date_month_ohe = OneHotEncoder()
created_date_day_ohe = OneHotEncoder()
created_date_dow_ohe = OneHotEncoder()
created_date_woy_ohe = OneHotEncoder()
X = created_date_year_ohe.fit_transform(df.created_date_year_encoded.values.reshape(-1,1)).toarray()
Xm = created_date_month_ohe.fit_transform(df.created_date_month_encoded.values.reshape(-1,1)).toarray()
Xo = created_date_day_ohe.fit_transform(df.created_date_day_encoded.values.reshape(-1,1)).toarray()
Xt = created_date_dow_ohe.fit_transform(df.created_date_dow_encoded.values.reshape(-1,1)).toarray()
Xz = created_date_woy_ohe.fit_transform(df.created_date_woy_encoded.values.reshape(-1,1)).toarray()

dfOneHot = pd.DataFrame(X, columns = ["created_date_year_"+str(int(i)) for i in range(X.shape[1])])
df = pd.concat([df, dfOneHot], axis=1)
dfOneHot = pd.DataFrame(Xm, columns = ["created_date_month_"+str(int(i)) for i in range(Xm.shape[1])])
df = pd.concat([df, dfOneHot], axis=1)
dfOneHot = pd.DataFrame(Xo, columns = ["created_date_day_"+str(int(i)) for i in range(Xo.shape[1])])
df = pd.concat([df, dfOneHot], axis=1)
dfOneHot = pd.DataFrame(Xt, columns = ["created_date_dow_"+str(int(i)) for i in range(Xt.shape[1])])
df = pd.concat([df, dfOneHot], axis=1)
dfOneHot = pd.DataFrame(Xz, columns = ["created_date_woy_"+str(int(i)) for i in range(Xz.shape[1])])
df = pd.concat([df, dfOneHot], axis=1)
