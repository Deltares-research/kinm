# -*- coding: utf-8 -*-
"""
Created on Fri Jan  8 14:06:35 2021

@author: Gerrit Hendriksen
"""
#  Copyright notice
#   --------------------------------------------------------------------
#   Copyright (C) 2021 Deltares for KINM (KennisImpuls Nutrienten Maatregelen)
#   Gerrit Hendriksen@deltares.nl
#   Kevin Ouwerkerk
#
#   This library is free software: you can redistribute it and/or modify
#   it under the terms of the GNU General Public License as published by
#   the Free Software Foundation, either version 3 of the License, or
#   (at your option) any later version.
#
#   This library is distributed in the hope that it will be useful,
#   but WITHOUT ANY WARRANTY; without even the implied warranty of
#   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#   GNU General Public License for more details.
#
#   You should have received a copy of the GNU General Public License
#   along with this library.  If not, see <http://www.gnu.org/licenses/>.
#   --------------------------------------------------------------------
#
# This tool is part of <a href="http://www.OpenEarth.eu">OpenEarthTools</a>.
# OpenEarthTools is an online collaboration to share and manage data and
# programming tools in an open source, version controlled environment.
# Sign up to recieve regular updates of this function, and to contribute
# your own tools.

# load sytem packages 
import json
import requests
from datetime import datetime
import os

# conda packages
import pandas as pd
import numpy as np
import configparser 

# third party packages
from sqlalchemy.sql.expression import update
from sqlalchemy import exc,func
from orm_timeseries import Base,FileSource,Location,Parameter,Unit,TimeSeries,TimeSeriesValuesAndFlags,Flags,Transaction

# local procedures
from kinm_helpders import establishconnection, read_config, loadfilesource,location,sparameter,sserieskey,sflag,settransaction

# set dirname for relative paths
# os.getcwd() is eerst handig om het regel voor regel te laten werken


def findlatestentry(session,tskey):
    ld=session.query(func.max(Transaction.periodend)).filter(Transaction.timeserieskey==tskey).scalar()
    ## ld should only be none when DB is still empty, if empty add fake date. 
    if ld == None:
        ld = datetime(year=1970, month= 1, day=1)
    return ld


### setup connection to api with trailer observations
# define location of api 
machineapi = 'https://data.talk2m.com/getewons'
dataapi = 'https://data.talk2m.com/getdata'
syncdataapi = 'https://data.talk2m.com/syncdata'
local = False
if not local:
    dirname =  os.path.dirname(__file__) 
    fc = os.path.join(dirname,'connection_kinm.txt')
else:
    dirname = r'C:\projecten\kinm\kinm_data'
    fc = os.path.join(dirname,'connection_local.txt')

# read config file and get devid and token
t2mconnection = os.path.join(dirname,'t2m_api_connection.txt')
cf = read_config(t2mconnection)

# get the machinename. As far as known by the time (2020-01-15) there is no 
# geographic reference for any machine name available, should come from the installation procedure
# for each machine in the machines set the filesource and the location
headers = {
    't2mdevid': cf.get('m2web', 'm2wdevid'),
    't2mtoken': cf.get('m2web', 'm2wtoken')
           }
machine = requests.post(machineapi, data = headers).json()

# get filesourcekey and lastTransactionID
filesourcekey,lasttransactionid = loadfilesource(machineapi, 
                                fc, 
                                'Meettrailer data Deltares - RIVM')

syncheaders = {'t2mdevid': cf.get('m2web', 'm2wdevid'),
               't2mtoken': cf.get('m2web', 'm2wtoken'),
               'createTransaction':True,
               'lastTransactionId':lasttransactionid}


## setup engine to the database
session,engine = establishconnection(fc)

## enable connection to dataapi
response = requests.post(syncdataapi, data = syncheaders)

trailer = response.json()
# trailer['moreDataAvailable']
lasttransactionid = trailer['transactionId']
# trailer['ewons'][0]['tags'][1]


tz = trailer['ewons'][0]['timeZone']
last = pd.to_datetime(trailer['ewons'][0]['lastSynchroDate'])

## een hulp dict om de parameters en unit tabel in te kunnen vullen
## name,(unit, unitdescription),description,shortname
## name of the paramter
## (unit, unitdescription) = unit and unitdescription
dctparams = {}
dctparams['troebelheid'] = ('troebelheid',('NTU','Nephelometric Turbidity Unit'),'Mate van ondoorzichtigheid van een op zichzelf heldere vloeistof veroorzaakt door de aanwezigheid van fijn verdeeld zwevend materiaal.', 'TROEBHD')
dctparams['tp'] = ('tp',('mg/l','milligram per liter'),'Het totaal van fosfor uit fosforhoudende verbindingen.', 'Ptot')
dctparams['ammonium'] = ('ammonium',('mg/l','milligram per liter'),'Ammonium is een eenwaardig polyatomisch kation met samenstelling NH₄⁺.', 'NH4')
dctparams['ph'] = ('ph',('ph','zuurgraad'),'Zuurgraad', 'pH')
dctparams['ec'] = ('ec',('mS/cm','millisiemens per centimeter'),'De elektrische geleidbaarheid van water.', 'GELDHD')
dctparams['zuurstof'] = ('zuurstof',('mg/l','milligram per liter'),'Hoeveelheid zuurstof opgelost in water.', 'O2')
dctparams['temperatuur'] = ('temperatuur',('oC','graad Celsius'),'Temperatuur van het water.', 'T')
dctparams['nitraat'] = ('nitraat',('mg/l','milligram per liter'),'Hoeveelheid nitraat opgelost in het water (uitgedrukt als stikstof).', 'NO3')
dctparams['regen'] = ('regen',('mm','millimeter'),'Neerslag', 'NEERSG')
dctparams['trp'] = ('trp',('mg/l','milligram per liter'),'Het reactieve fosfor gedeelte uit fosforhoudende verbindingen dat oplost in zuur.', '')
dctparams['pump'] = ('pump',('',''),'Geeft aan of de pomp aan (1) of uit (0) staat.', '')

dctlocation={}
dctlocation['Meettrailer01_RD'] = (168780,503914,'Vuursteentocht')
dctlocation['Meettrailer02_RD'] = (187456,398987,'Vinkenloop')

for i in range(0, len(trailer['ewons'])):
    
    trailerid = trailer['ewons'][i]['id']
    ## assumption is that machineapi and dataapi are of same size 
    ## (with respect to the number of trailers, of course not regarding
    ## amount of data, but that is a different tag. Main tags should be the same size,
    ## e.g. the machines. So the trailerid's should be similar)

    locationkey = location(fc,
                           filesourcekey,
                           trailer['ewons'][i]['name'],
                           dctlocation[trailer['ewons'][i]['name']][0],
                           dctlocation[trailer['ewons'][i]['name']][1],
                           28992,
                           trailerid,
                           dctlocation[trailer['ewons'][i]['name']][2])
    sub = trailer['ewons'][i]['tags']
    try:
        for j in range(len(sub)):
            # get or insert parameterkey
            aparameter = str(sub[j]['name'])
            print(aparameter)
            
            if aparameter.find('Alm') != -1:
                print("alarm found as parameter. The steps to add a timeseries will be skipped",aparameter)
                continue
            else:
                print('data will be added for:', aparameter )
            
            parameterkey = sparameter(fc,
                             aparameter,
                             dctparams[aparameter][0],
                             dctparams[aparameter][1],  # unit is tuple of shortnotation and description (e.g. m and meter)
                             dctparams[aparameter][2],
                             dctparams[aparameter][3],compartment='OW')    
            ## set or get the flag for quality based on the sub['quality'] element
            flagkey = sflag(fc,sub[j]['quality'])
        
            ## haalop of maak aan een timeseries key (uniek voor locatie, parameteter, timestep)
            ## functie nog maken. return moet zijn tskey
            tskey = sserieskey(fc,parameterkey,locationkey,filesourcekey,timestep='nonequidistant')
            
            print(sub[j]['id'])
            try:
                df = pd.DataFrame(sub[j]['history'])
            except:
                print("history is empty, parameter is: " + sub['name'])
            
            ## check if last inserted date in DB overlaps with dates in the dataframe
            ld = findlatestentry(session,tskey)
            fd = datetime.strptime(df['date'].iloc[0], "%Y-%m-%dT%H:%M:%SZ")
            if ld > fd:
                print('')
                print('---------------------------------------------------------------------------------------------------------')
                print('No data added for {p} for trailer {t} for date period'.format(t=machine['ewons'][i]['name'],p=aparameter), str(df['date'].iloc[0]), str(df['date'].iloc[-1]))                
                continue
            elif ld == fd or ld < fd:
                print('data can be added')
            
            # aanvullen df met tskey en rename columns
            dfsql = df[['date','value']]
            dfsql['timeserieskey'] = tskey
            
            if aparameter == 'pump':
                #dan stop de data in de tabel timeseriespumpshistory
                dfsql.rename(columns={'date':'datetime','value':'pumpvalue'},inplace=True)
                dfpump = dfsql
                dfpump.to_sql('timeseriespumphistory',engine,index=False,if_exists='append',schema='timeseries')
            else:
                dfsql['flags'] = flagkey
                dfsql.rename(columns={'date':'datetime','value':'scalarvalue'},inplace=True)
               
                # store data in timeseriesvalues and flags table
                dfsql.to_sql('timeseriesvaluesandflags',engine,index=False,if_exists='append',schema='timeseries')
                settransaction(tskey,np.min(df['date']), np.max(df['date']), lasttransactionid, session)
                print('')
                print('---------------------------------------------------------------------------------------------------------')
                print('Succesfully added data for {p} for trailer {t} for date period'.format(t=machine['ewons'][i]['name'],p=aparameter), str(dfsql['datetime'].iloc[0]), str(dfsql['datetime'].iloc[-1]))                
                
        stmt=update(FileSource).where(filesourcekey==filesourcekey).values(lasttransactionid=lasttransactionid)
        engine.execute(stmt)

    except exc.SQLAlchemyError as err:
        # pass exception to function
        print('dbase sqlalchemy error')
        print(err)        
    except:
        print("exception raised while adding data for",machine['ewons'][i]['name'])
    finally:
        session.close()
        engine.dispose()

# f = open(r'C:\projecten\kinm\log.txt','a+')
# f.write(' '.join(['task run and loaded data for the period',str(dfsql['datetime'].iloc[0]), str(dfsql['datetime'].iloc[-1])])+'\r\n')
# f.close()