# -*- coding: utf-8 -*-
"""
Created on Tue Feb  9 14:55:27 2021

@author: hendrik_gt
"""

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

import os
import pandas as pd
import requests
import time
from datetime import datetime
import configparser
import logging

# third party packages
from sqlalchemy.sql.expression import update
from sqlalchemy import exc, func

# local procedures
from orm_timeseries import (
    Base,
    FileSource,
    Location,
    Parameter,
    Unit,
    TimeSeries,
    TimeSeriesValuesAndFlags,
    Flags,
)
from kinm_helpders import (
    establishconnection,
    read_config,
    loadfilesource,
    location,
    sparameter,
    sserieskey,
    sflag,
    dateto_integer,
    convertlttodate,
)


def setparameter(fc, obstype, wns):
    quantity = obstype["quantity"]
    parameterCode = obstype["parameterCode"]
    unit = obstype["unit"]
    compartment = "Undefined"
    if "compartment" in obstype.keys():
        compartment = obstype["compartment"]
    pkey = sparameter(
        fc,
        parameterCode,
        quantity,
        (unit, ""),
        parameterCode,
        compartment=compartment,
        wns=wns,
    )
    return pkey


# 1. Get connection details to the database
local = True
if local:
    fc = r"D:\documents\kinm_notes\kinm\connection_kinm.txt"
else:
    dirname = os.path.dirname(__file__)
    fc = os.path.join(dirname, "connection_kinm.txt")
    fcproxies = os.path.join(dirname, "proxieconfig.txt")
session, engine = establishconnection(fc)

# 3 Get locations
data_api = "https://zuiderzeeland.lizard.net/dd/api/v2"
wkt = "MULTIPOLYGON(((5.60232785040067 52.5626330313122,5.60362653539707 52.5412443762573,5.63048860711355 52.5423532036001,5.63049050908842 52.5423532805844,5.6304908777627 52.5423506375709,5.63055852829833 52.5422741788991,5.63055833662209 52.542274170305,5.63137717444563 52.5371863099332,5.63189764522966 52.5339153309158,5.6316804836226 52.5338091805669,5.63168042671451 52.533809549163,5.6164717915904 52.5328414750194,5.61739912589349 52.5240275019206,5.61518367450573 52.523926702785,5.60899029849347 52.5237023889268,5.60288600821504 52.5234231178189,5.59990832898269 52.5232832264673,5.59634939194729 52.5231287930842,5.59222913273955 52.5229958059194,5.593356101131 52.5148126970772,5.5898141333448 52.5149628884301,5.58860145178966 52.5228476310487,5.56936681585104 52.5220851685143,5.56882551972037 52.5309188474822,5.568243938402 52.5403787008571,5.56823610443199 52.5405062060503,5.56823570391527 52.5405127673595,5.56823356204983 52.5405475513147,5.56802801378877 52.5438899010369,5.56755922270831 52.5495912239822,5.56707255283716 52.5509421066955,5.56690106399305 52.5509946993246,5.5663533159301 52.5511626815721,5.56873089537632 52.5527290192399,5.56958025510695 52.5531337054452,5.57834358857819 52.5586945583736,5.57910665686556 52.5591786812552,5.57994098946444 52.5595262753971,5.58070255312361 52.5599828625552,5.58386521885919 52.5618788447204,5.5860633304624 52.5628974223077,5.58976589695541 52.5636774944937,5.597083383622 52.5648180772766,5.59717381056749 52.5649492564154,5.60145286305604 52.565631303093,5.60163328824954 52.5656600507789,5.60233418199313 52.5639297765778,5.60232785040067 52.5626330313122)))"
locations_api = data_api + "/locations/?geom_within=" + wkt
locations = requests.get(locations_api).json()["results"]


# 2. Get filesource key and last transaction id
filesource_key, last_transaction_id = loadfilesource(
    data_api, fc, "link to FEWS DDAPI of waterboard Zuiderzeeland"
)

if last_transaction_id is None:
    start_date = "2019-01-01T00:00:00Z"
else:
    start_date = convertlttodate(last_transaction_id, ddapi=True)

# NOTE test
# start_date = "2021-02-12T15:15:00Z"
end_date = datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ")
# start_date = "2021-02-12T15:15:00Z"
# end_date = "2021-02-14T15:15:00Z"


for loc in locations:

    location_name = loc["properties"]["locationName"]
    location_code = loc["properties"]["locationCode"]
    location_geom = loc["geometry"]["coordinates"]
    location_key = location(
        fc,
        filesource_key[0],
        location_name,
        location_geom[0],
        location_geom[1],
        4326,
        location_code,
    )
    location_ts_data_api = data_api + "/timeseries/?locationCode=" + location_code
    test_locations = requests.get(location_ts_data_api).json()
    location_ts_data = requests.get(location_ts_data_api).json()["results"]

    for ts in location_ts_data:

        observation_type = ts["observationType"]
        qualifier = ts["qualifier"]
        parameter_key = setparameter(fc, observation_type, wns=qualifier)
        flag_key = sflag(fc, "no flag")
        timeseries_key = sserieskey(
            fc,
            parameter_key,
            location_key,
            filesource_key[0],
            timestep="nonequidistant",
        )
        timeseries_url = ts["url"]
        events = requests.get(
            timeseries_url
            + "?startTime="
            + str(start_date)
            + "&endTime="
            + str(end_date)
        ).json()["events"]
        print(
            f""" 
                ------------
                location code: {location_code}
                location_name: {location_name}
                timeseries_key: {timeseries_key}
                parameter: {observation_type["quantity"]}
        """
        )
        if events:
            df = pd.DataFrame(events)
            print("-----")
            print(f"events in dataframe form: {df}")
            data_timestamp = datetime.strptime(
                df["timeStamp"].iloc[0], "%Y-%m-%dT%H:%M:%SZ"
            )

            dfsql = df[["timeStamp", "value"]]
            dfsql["timeserieskey"] = timeseries_key
            dfsql["flags"] = flag_key
            dfsql.rename(
                columns={"value": "scalarvalue", "timeStamp": "datetime"}, inplace=True
            )
            print(f"dataframe to sql is: {dfsql}")
            # NOTE to improve speed if last_transaction_id is None which means that
            # it is the first time that we run the script then put the
            if last_transaction_id is None:
                try:
                    dfsql.to_sql(
                        "timeseriesvaluesandflags",
                        engine,
                        index=False,
                        if_exists="append",
                        schema="timeseries",
                    )
                except exc.IntegrityError as e:
                    print(f"Exception: {e}")
            else:
                for i in range(len(dfsql)):
                    try:
                        dfsql.iloc[i : i + 1].to_sql(
                            "timeseriesvaluesandflags",
                            engine,
                            index=False,
                            if_exists="append",
                            schema="timeseries",
                        )
                    except exc.IntegrityError as e:
                        print(f"Exception: {e}")

        else:
            print(f'No events found for {observation_type["quantity"]}')

end_date = dateto_integer(end_date, ddapi=True)
stmt = (
    update(FileSource)
    .where(FileSource.filesourcekey == filesource_key)
    .values(lasttransactionid=int(end_date))
)
engine.execute(stmt)

session.close()
engine.dispose()
