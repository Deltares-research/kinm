# KINM_data

The scripts and setup for the data streams of the KINM project. 

This project intends to retrieve data from various sources from 2 meusurment trailers setup within the project KINM (KennisImpuls Nutrienten Maatregelen) of BGS-BGK. Joachim Rozemeijer is project manager and Gerrit Hendriksen and Kevin Ouwerkerk are developers for this project.
The retrieved data is being stored in a Deltares PostgreSQL/PostGIS database. This database is an internal Deltares database for now only accesible by Kevin Ouwerkerk, Gerrit Hendriksen and members of the waterkwaliteits team. 

# setup of the database
As datamodel a simplified datamodel derived from FEWS Timeseries datamodel is used. This simplified datamodel is created by the scripts orm_timeseries and orm_initializeseries.py. The following record is the connection string
- postgres://yourusername:yourpassword@openearth-deltares-dataportal-db.avi.directory.intra/KINM

# datasources and their scripts. These will be described by the location/maintainer of the data

---- Waterschap Zuiderzeeland - Vuursteentocht
- Waterboard Zuiderzeeland offers a FEWS api accessible via https://zuiderzeeland.lizard.net/api/v3/
- the script getlocationsviaapi.py is a first attempt to access the api and retrieves the data

---- Waterschap Aa en Maas - Vinkenloop
- Waterschap Aa en Maas configure an Azure facility that offers data that can be accessed via a container accessible by
https://amrsg01.blob.core.windows.net/datasets. The token is sv=2019-12-12&si=29c44c39-b96e-40ad-a14b-83885f47d38e&sr=c&sig=5OHG7fDrA/Sn0Sg8D8cB9/XDT1FADTjc6mDFe%2BcnOmk%3D
- ths first version of the script is called readazure.py and downloads all data available to disk by parameter

---- Measurement trailer
By Deltares and RIVM with STOWA as partner 2 measurement trailers have been constructed. These send data to ewon online device. The api token to get access is 4dxbA0D0475DzEEBLTz8z6DPkvD8yzCYyXUOGkjnlJWcwYCXmA
Documentation can be found at https://developer.ewon.biz/system/files_force/rg-0005-00-en-reference-guide-for-dmweb-api.pdf?download=1.
