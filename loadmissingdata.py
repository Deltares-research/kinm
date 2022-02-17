# -*- coding: utf-8 -*-
"""
Created on Mon May 10 16:57:45 2021

@author: hendrik_gt

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

# this script loads data for a given period, so for a from data to an end date.

# find first entry of certain parameter

import os
import pandas as pd
from datetime import datetime, timedelta
from dateutil.rrule import rrule, DAILY
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine, func, update, insert
from sqlalchemy.orm import sessionmaker

from orm_timeseries import (
    Base,
    FileSource,
    Location,
    Parameter,
    Unit,
    TimeSeries,
    TimeStep,
    Flags,
    TimeSeriesPumpHistory,
    Transaction,
)
from kinm_helpders import establishconnection

local = True
if not local:
    # dirname =  os.path.dirname(__file__)
    dirname = r"C:\projecten\kinm\kinm_data"
    fc = os.path.join(dirname, "connection_kinm.txt")
else:
    #dirname = r"D:\documents\kinm-data"
    dirname = r"C:\Users\ouwerker\OneDrive - Stichting Deltares\Documents\KINM\kinm_repo\kinm"  # \kinm-data
    fc = os.path.join(dirname, "connection_kinm.txt")

# enable connection:
session, engine = establishconnection(fc)


def checkdate(engine, p, l, date):
    if p != "pump":
        stmt = """select count(*) from timeseries.timeseriesvaluesandflags tsv
        join timeseries.timeseries ts on ts.timeserieskey = tsv.timeserieskey
        join timeseries.parameter p on p.parameterkey = ts.parameterkey
        join timeseries.location l on l.locationkey = ts.locationkey 
        where p.id = '{p}' and l.name = '{l}' and datetime::date = '{d}'::date""".format(
            p=p, l=l, d=date
        )
    else:
        stmt = """select count(*) from timeseries.timeseriespumphistory tsp
        join timeseries.timeseries ts on ts.timeserieskey = tsp.timeserieskey
        join timeseries.parameter p on p.parameterkey = ts.parameterkey
        join timeseries.location l on l.locationkey = ts.locationkey 
        where p.id = '{p}' and l.name = '{l}' and datetime::date = '{d}'::date""".format(
            p=p, l=l, d=date
        )
    ld = engine.execute(stmt).fetchall()
    return ld

# mltiple date parsers
def combine_date_parsers(date_parsers):
    def combined_date_parser(value):
        for date_parser in date_parsers:
            try:
                return date_parser(value)
            except ValueError:
                pass
        else:
            raise ValueError(value)
    return combined_date_parser

# date parsers
mydateaparser1 = lambda x: datetime.strptime(x, "%d-%m-%Y %H:%M:%S")
mydateaparser2 = lambda x: datetime.strptime(x, "%m/%d/%Y %H:%M:%S")
mydateaparser3 = lambda x: datetime.strptime(x, "%d-%m-%Y %H:%M")

mydateaparser = combine_date_parsers([
    mydateaparser1,
    mydateaparser2,
    mydateaparser3,
])


def data2df(datadir, l, location, date, columns, fkey, p):
    adf = os.path.join(
        datadir, location, "".join(["data", date.strftime("%Y%m%d"), ".csv"])
    )
    print("ingesting", adf)

    loc = session.query(Location).filter_by(name=l).first()

    columnnames = columns.copy()
    columnnames.insert(0, "date")
    columnnames.insert(1, "empty")
    if os.path.isfile(adf):

        df = pd.read_csv(
            adf,
            sep=";",
            names=columnnames,
            parse_dates=[0],
            date_parser=mydateaparser,
            index_col=False,
            skiprows=3,
            na_values=["1.#R"],
            encoding='latin_1'
        )

        # prepare for ingestion for specific parameter, for specific location and date
        pky = session.query(Parameter).filter_by(id=p).first()
        tsk = session.query(TimeSeries).filter_by(
            filesourcekey=fkey,
            locationkey=loc.locationkey,
            parameterkey=pky.parameterkey,
        )
        dft = df[["date", p]].copy()
        dft["timeserieskey"] = tsk[0].timeserieskey
        dft.dropna(how="any", inplace=True)

        if p != "pump":
            dft.rename(columns={"date": "datetime", p: "scalarvalue"}, inplace=True)
            dft["flags"] = 3
            dft.to_sql(
                "timeseriesvaluesandflags",
                engine,
                schema="timeseries",
                if_exists="append",
                index=False,
            )
        else:
            dft.rename(columns={"date": "datetime", p: "pumpvalue"}, inplace=True)
            dft.to_sql(
                "timeseriespumphistory",
                engine,
                schema="timeseries",
                if_exists="append",
                index=False,
            )
    return


# from date, so this date will be included also, at least checked
fromdate = datetime.strptime("20210219", "%Y%m%d").date()
#todate = datetime.strptime("20220101", "%Y%m%d").date()
now = datetime.now()
todate = now.date()

datadir = r"N:\Projects\11202000\11202460\B. Measurements and calculations\R\data_downloads_trailers"
#datadir = r"C:\projecten\kinm\kinm_data\temp\meetwagen"

dctlocation = {}
dctlocation["Meettrailer01_RD"] = "trailer1"
dctlocation["Meettrailer02_RD"] = "trailer2"
lstparams = [
    "tp",
    "trp",
    "ammonium",
    "ph",
    "ec",
    "troebelheid",
    "zuurstof",
    "temperatuur",
    "nitraat",
    "regen",
    "pump",
]
columns = lstparams

# derive the filesourceky
fkey = (
    session.query(FileSource)
    .filter_by(remark="Meettrailer data Deltares - RIVM")
    .first()
)

for l in dctlocation.keys():
    for p in lstparams:
        for dt in rrule(DAILY, dtstart=fromdate, until=(todate - timedelta(days=1))):
            novals = checkdate(engine, p, l, dt)[0][0]
            if novals == 0:
                data2df(
                    datadir, l, dctlocation[l], dt, lstparams, fkey.filesourcekey, p
                )
            else:
                print("al in de database", dt, dctlocation[l], p)

session.close()
engine.dispose()
