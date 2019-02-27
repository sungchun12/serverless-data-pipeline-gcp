"""
Add a description here
"""

#built in python modules
import os
from datetime import datetime
#gcp modules
from gcloud import storage
from google.cloud import bigquery
import pandas_gbq as gbq
from google.auth import compute_engine
#api module
from sodapy import Socrata
#pandas dataframe module
import pandas as pd

def create_bucket(bucket_name):
    """Detects whether or not a new bucket needs to be created"""
    client = storage.Client.from_service_account_json('service_account.json') #authenticate service account
    bucket = client.bucket(bucket_name) #capture bucket details
    bucket.location = 'US-CENTRAL1' #define regional location
    if not bucket.exists(): #checks if bucket doesn't exist
        bucket.create()
        print("Created a new bucket: {0}".format(bucket_name))
    else:
        print("Bucket already exists")