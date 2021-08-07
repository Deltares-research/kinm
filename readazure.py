# -*- coding: utf-8 -*-
"""
Created on Tue Oct 27 20:43:04 2020

@author: hendrik_gt
"""

import os, uuid, sys
import pandas as pd
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient

prolog = "projecten/sensor_gestuurd_boeren/"
dirnm = r"N:\Projects\11202000\11202460\B. Measurements and calculations\R\data_streams\kinm_data"
# dirnm = os.path.dirname(__file__)
path = os.path.join(dirnm, r'temp\meetwagen') 

# Door Datalab verstrekt:
container_url = "https://amrsg01.blob.core.windows.net/datasets"
sas_token = "sv=2019-12-12&si=29c44c39-b96e-40ad-a14b-83885f47d38e&sr=c&sig=5OHG7fDrA/Sn0Sg8D8cB9/XDT1FADTjc6mDFe%2BcnOmk%3D"

container_client = ContainerClient.from_container_url(container_url, sas_token)

# Process Container
blobs = container_client.list_blobs(name_starts_with= prolog)
for item in blobs:
    print(item.name)

# Process a Blob
example = prolog + "sensor_data_fews/pH.meting.millivolt.csv"
localfilename = example.split("/")[-1]

# Process Container
blobs = container_client.list_blobs(name_starts_with= prolog)
for item in blobs:
    blob = container_client.get_blob_client(blob=item.name)
    fn = os.path.join(path,item.name.split("/")[-1])
    with open(fn, "wb+") as file:
        file.write(blob.download_blob().readall())

# OR create blob_client independent from container_client
# blob_client = BlobClient.from_blob_url("{}?{}".format(container_url, sas_token))

fn = os.path.join(path,localfilename)
with open(fn, "wb+") as file:
    file.write(blob.download_blob().readall())
    
df = pd.read_csv(fn,sep=',')
