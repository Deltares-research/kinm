# -*- coding: utf-8 -*-
"""
Created on Fri Jan 29 17:38:11 2021

@author: hendrik_gt
"""
import json
import requests
import time
import datetime
import os
import matplotlib.pyplot as plt

# conda packages
import pandas as pd
import configparser 

from kinm_helpders import establishconnection


local = False
if not local:
    dirname =  os.path.dirname(__file__) 
    fc = os.path.join(dirname,'connection_kinm.txt')
else:
    dirname = r'D:\projecten\datamanagement\Nederland\waterkwaliteit\kinm_data'
    fc = os.path.join(dirname,'connection_local.txt')

session,engine = establishconnection(fc)

params = ('troebelheid','tp','ammonium','ph','ec','zuurstof','temperatuur','nitraat','regen','trp')
trailers = ('Meettrailer01_RD','Meettrailer02_RD')
for trailer in trailers:
    for parameter in params:
        strsql = """SELECT datetime,scalarvalue FROM timeseries.timeseriesvaluesandflags tsv 
        join timeseries.timeseries ts on ts.timeserieskey = tsv.timeserieskey
        join timeseries.parameter p on p.parameterkey = ts.parameterkey
        join timeseries.location l on l.locationkey = ts.locationkey
        where l.name = '{t}' and p.name = '{p}' and datetime > to_timestamp('2021-02-2 00', 'YYYY-MM-DD HH24')
        order by datetime""".format(p=parameter,t=trailer)
        
        df = pd.read_sql(strsql,engine)
        df.plot(kind="line", x="datetime", y="scalarvalue",title=' '.join([parameter,'for trailer',trailer] ))
        plt.show()
        
session.close()
engine.dispose()

#

# strsql = """SELECT datetime, pumpvalue FROM timeseries.timeseriespumphistory tsv 
# join timeseries.timeseries ts on ts.timeserieskey = tsv.timeserieskey
# join timeseries.parameter p on p.parameterkey = ts.parameterkey
# join timeseries.location l on l.locationkey = ts.locationkey
# where l.name in ('Meettrailer01_RD') and p.name = 'pump'
# order by datetime
# """
# df = pd.read_sql(strsql,engine)
# df.plot(kind="line", x="datetime", y="pumpvalue",title='pump indicator')
# plt.show()

