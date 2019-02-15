#!/usr/bin/env python

# make sure to install these packages before running:
# pip install pandas
# pip install sodapy
# pip install google-cloud-storage
# pip install google-cloud-bigquery
# pip install pandas-gbq

import pandas as pd
import os
from google.cloud import bigquery
import google.cloud.storage
from sodapy import Socrata
from pandas.io import gbq
from datetime import datetime

def _getToday():
  return datetime.now().strftime('%Y%m%d%H%M%S')

# os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="./service_account.json"

def handler(request):
	"""
	Creates a bucket, dataset, empty table, 
	and appends Chicago Traffic API data into empty table
	"""
	#environment variables
	# bucket_name = os.environ['BUCKET_NAME'] #capture bucket name where raw data will be stored
	# dataset_name = os.environ['DATASET_NAME'] #initial dataset
	# table_name = os.environ['TABLE_NAME'] #name of table to capture data

	bucket_name = 'test_bucket' #capture bucket name where raw data will be stored
	dataset_name = 'test_dataset' #initial dataset
	table_name = 'test_table' #name of table to capture data	

	# Unauthenticated client only works with public data sets. Note 'None'
	# in place of application token, and no username or password:
	client = Socrata("data.cityofchicago.org", None)

	# Google Cloud Storage client creation and information
	#storage_client = google.cloud.storage.Client()
	storage_client = google.cloud.storage.Client.from_service_account_json(
					'service_account.json')
	# bucket_name = 'chi-traffic'
	bucket = storage_client.get_bucket(bucket_name)

	# Create storage bucket if it does not exist
	if not bucket.exists():
		bucket.create()

	# Create a BigQuery client and information
	bigquery_client = bigquery.Client()
	# dataset_name = 'chicago_traffic'
	# table_name = 'chitraffic'

	dataset = bigquery_client.dataset(dataset_name)
	table = dataset.table(table_name)

	# First 2000 results, returned as JSON from API / converted to Python list of
	# dictionaries by sodapy.
	results = client.get("8v9j-bter", limit=2000)

	# Convert to pandas DataFrame
	results_df = pd.DataFrame.from_records(results)

	# Write the DataFrame to GCS (Google Cloud Storage)
	results_df.to_csv('traffic' + _getToday() + '.csv')
	source_file_name = 'traffic' + _getToday() + '.csv'
	blob = bucket.blob(os.path.basename(source_file_name))
	blob.upload_from_filename(source_file_name)

	# print('File {} uploaded to {}.'.format(
	# 				source_file_name,
	# 				bucket))
	# Write the DataFrame to a BigQuery table
	gbq.to_gbq(results_df, 'chicago_traffic.chitraffic', 'iconic-range-220603', if_exists='append')
	return f'Uploaded raw csv file to bucket: {0}, and appended data to BigQuery dataset: {1}, table: {2} on '.format(bucket_name,dataset_name,table_name) + _getToday()
