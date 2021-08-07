# -*- coding: utf-8 -*-
"""
Created on Mon Jul 27 17:07:02 2020

@author: hendrik_gt

#  Copyright notice
#   --------------------------------------------------------------------
#   Copyright (C) 2021 Deltares for Projects with a FEWS datamodel in 
#                 PostgreSQL/PostGIS database used in KINM project
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
"""

import pandas as pd
import requests
import time
import datetime

# third party packages
from sqlalchemy.sql.expression import update

# local procedures
from orm_timeseries import Base,FileSource,Location,Parameter,Unit,TimeSeries,TimeSeriesValuesAndFlags,Flags
from kinm_helpders import establishconnection, read_config, loadfilesource,location,sparameter,sserieskey,sflag,dateto_integer,convertlttodate


# data is stored in PostgreSQL/PostGIS database. A connection string is needed to interact with the database. This is typically stored in
# a file.
local = True
if local:
    fc = fc = r"D:\projecten\datamanagement\Nederland\waterkwaliteit\kinm_data\connection_local.txt"
else:
    fc = r"C:\projecten\kinm\tools\connection_kinm.txt"
session,engine = establishconnection(fc)

# add filesoure --> source of information
api = 'https://zuiderzeeland.lizard.net/api/v3/'

# call the procedure in kinm_helpers.py that stores the source in the filesource table
fkey = loadfilesource(api, fc, 'link to FEWS API of waterboard Zuiderzeeland')

# get the transactionid, for this specific filesource the transaction id is the last time in integernumber
result = session.query(FileSource).filter_by(filesource=api).first()
atime = result.lasttransactionid
if atime==None:
    starttime = '2019-01-01 01:00:00'
    st = dateto_integer(starttime)
else:
    st = atime

# for the logs
starttime = convertlttodate(st)
enddate = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
et = dateto_integer(enddate)

# getting the data, a first example using the uuid of the combination parameter/location
# including starttime and endtime in unix timestamp
def gettimeseries(uuid,st,et):
    daturl = 'https://zuiderzeeland.lizard.net/api/v3/timeseries/?end={et}&start={st}&uuid={uuid}'.format(uuid=uuid,st=int(st),et=int(et))
    tresponse = requests.get(daturl).json()
    results = tresponse['results'][0]['events']
    #title = tresponse['results'][0]['observation_type']['parameter']
    df = pd.DataFrame(results)
    return df

# setup connection to FEWS Api and use the polygon of the area.
wkt = 'MULTIPOLYGON(((5.60232785040067 52.5626330313122,5.60362653539707 52.5412443762573,5.63048860711355 52.5423532036001,5.63049050908842 52.5423532805844,5.6304908777627 52.5423506375709,5.63055852829833 52.5422741788991,5.63055833662209 52.542274170305,5.63137717444563 52.5371863099332,5.63189764522966 52.5339153309158,5.6316804836226 52.5338091805669,5.63168042671451 52.533809549163,5.6164717915904 52.5328414750194,5.61739912589349 52.5240275019206,5.61518367450573 52.523926702785,5.60899029849347 52.5237023889268,5.60288600821504 52.5234231178189,5.59990832898269 52.5232832264673,5.59634939194729 52.5231287930842,5.59222913273955 52.5229958059194,5.593356101131 52.5148126970772,5.5898141333448 52.5149628884301,5.58860145178966 52.5228476310487,5.56936681585104 52.5220851685143,5.56882551972037 52.5309188474822,5.568243938402 52.5403787008571,5.56823610443199 52.5405062060503,5.56823570391527 52.5405127673595,5.56823356204983 52.5405475513147,5.56802801378877 52.5438899010369,5.56755922270831 52.5495912239822,5.56707255283716 52.5509421066955,5.56690106399305 52.5509946993246,5.5663533159301 52.5511626815721,5.56873089537632 52.5527290192399,5.56958025510695 52.5531337054452,5.57834358857819 52.5586945583736,5.57910665686556 52.5591786812552,5.57994098946444 52.5595262753971,5.58070255312361 52.5599828625552,5.58386521885919 52.5618788447204,5.5860633304624 52.5628974223077,5.58976589695541 52.5636774944937,5.597083383622 52.5648180772766,5.59717381056749 52.5649492564154,5.60145286305604 52.565631303093,5.60163328824954 52.5656600507789,5.60233418199313 52.5639297765778,5.60232785040067 52.5626330313122)))'
lapi = 'https://zuiderzeeland.lizard.net/api/v3/locations/?geom_within='+wkt
lresponse = requests.get(lapi).json()
for l in range(len(lresponse['results'])):
    print(l,lresponse['results'][l]['code'],lresponse['results'][l]['name'],lresponse['results'][l]['geometry']['coordinates'])
    locationkey = location(fc,
                           fkey[0],
                           lresponse['results'][l]['name'],
                           lresponse['results'][l]['geometry']['coordinates'][0],
                           lresponse['results'][l]['geometry']['coordinates'][1],
                           4326,
                           lresponse['results'][l]['code'])
    
    # every station (code) has its own response dataset
    purl = 'https://zuiderzeeland.lizard.net/api/v3/measuringstations/?code={c}'.format(c=lresponse['results'][l]['code'])
    presponse = requests.get(purl).json()
    if presponse['count'] != 0:
        tstep = presponse['results'][0]['frequency']
        flagkey = sflag(fc,'not defined','within FEWS')
        if presponse['count'] != 0:
            lstts = presponse['results'][0]['timeseries']
            for i in lstts:
                # use this uuid --> the uuid that is combination of location and parameter measured,
                # should be used to get the timeseries via the endpoint timeseries, incl. start time and endtime (in Linux notation)
                print(i['parameter'],i['uuid'])
                # get or set parameterkey 
                pkey = sparameter(fc,i['parameter'],i['parameter'],(i['unit'], ''),'in'.join([i['parameter']]))
                
                # get or set serieskey
                skey = sserieskey(fc, pkey, locationkey, fkey[0],timestep=tstep)
                
                # read the data and load into database
                df = gettimeseries(i['uuid'],st,et)
                df['datetime'] = pd.to_datetime(df['timestamp'],unit='ms',origin='unix')
                dfsql = df[['datetime','min']]
                dfsql['timeserieskey'] = skey
                dfsql['flags' ] = flagkey
                dfsql.rename(columns={'min':'scalarvalue'},inplace=True)
                #dfsql.to_sql('tempfewsdata',engine,index=False,if_exists='append',schema='timeseries')
                dfsql.to_sql('timeseriesvaluesandflags',engine,index=False,if_exists='append',schema='timeseries')

# set the endtime
stmt=update(FileSource).where(FileSource.filesourcekey==fkey[0]).values(lasttransactionid=et)
engine.execute(stmt)

session.close()
engine.dispose()