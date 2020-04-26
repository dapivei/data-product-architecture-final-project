#!/usr/bin/env python

##----extracting holiday dates in the USA: period (2010-2020)---##

import holidays
import numpy as np
import pandas as pd
from datetime import timedelta, date

daterange = range(2010,2021)

us_holidays = []
for i in daterange:
    for date in holidays.US(years = i).items():
        us_holidays.append(str(date[0]))
print(us_holidays, end=',')
