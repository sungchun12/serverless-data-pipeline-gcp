#!/usr/bin/env python
"""
Add description here
# make sure to install these packages before running:
# these should all be in the requirements.txt file when deploying cloud function
# pip install pandas
# pip install sodapy
# pip install google-cloud-storage
# pip install google-cloud-bigquery
# pip install pandas-gbq
# pip install gcloud

#this is needed for local development in order to appropriately access GCP from a local service account key
#https://cloud.google.com/docs/authentication/production#auth-cloud-compute-engine-python
#may not use this at all and have cloud functions use the default service account: https://cloud.google.com/docs/authentication/production#auth-cloud-compute-engine-python
# os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="./service_account.json", used for jupyter notebook
# https://cloud.google.com/docs/authentication/getting-started#auth-cloud-implicit-python

#run the below in windomws command prompt for authentication
set GOOGLE_APPLICATION_CREDENTIALS=C:\Users\sungwon.chung\Desktop\cloud_function_poc\src\service_account.json
"""

#lib modules
#TODO: trim unnecessary imports from wildcards
from lib.schemas import schema_bq, schema_df
from lib.bq_api_data_functions import *
from lib.data_ingestion import *
# from lib.helper_functions import *
from lib.infrastructure_setup import *

#may want to define bigquery client, dataset_ref, and table_ref earlier in the handler function to avoid redundant code
#TODO: add in flask requests module to work properly with cloud function
def handler(schema_bq, schema_df):
	"""Main function that orchestrates the data pipeline from start to finish"""
	#define environment variables
	project_id = 'iconic-range-220603' #capture the project id to where this data will land
	bucket_name = 'chicago_traffic_raw' #capture bucket name where raw data will be stored
	dataset_name = 'chicago_traffic_demo' #initial dataset
	table_name = 'table_raw' #name of table to capture data
	table_desc = 'Raw, public Chicago traffic data is appended to this table every 5 minutes'#table description
	nulls_expected = ('_comments') #tuple of nulls expected for checking data outliers
	partition_by = '_last_updt' #partition by the last updated field for faster querying and incremental loads
	# timestamp = _getToday()
	create_bucket(bucket_name) #TODO: figure out why my service account file isn't working
	create_dataset_table(dataset_name, table_name, table_desc, schema_bq, partition_by)
	results_df = create_results_df()
	upload_raw_data_gcs(results_df, bucket_name)
	results_df_transformed = convert_schema(results_df, schema_df)
	print(results_df_transformed.dtypes)

	#check if there are any nulls in the columns except for _comments
	null_columns = check_nulls(results_df_transformed)
	null_outliers = check_null_outliers(null_columns, nulls_expected)

	#upload data to bigquery
	upload_to_gbq(results_df_transformed, project_id, dataset_name, table_name)
	bq_table_num_rows(dataset_name, table_name)

	#Preprocess data for unique records accumulation
	query_max_timestamp(project_id, dataset_name, table_name)
	#create a table for unique records staging
	table_name_2 = 'unique_records_staging'
	table_desc_2 = "Unique records from table: {}".format(table_name)
	create_dataset_table(dataset_name, table_name_2, table_desc_2, schema_bq, partition_by)
	#create a table for unique records final
	table_name_2 = 'unique_records_final'
	table_desc_2 = "Unique records from table: {}".format(table_name)
	create_dataset_table(dataset_name, table_name_2, table_desc_2, schema_bq, partition_by)
	table_name_2 = "unique_records_staging"
	query_unique_records(project_id, dataset_name, table_name, table_name_2)
	table_name = 'unique_records_staging'
	table_name_2 = 'unique_records_final'
	append_unique_records(project_id, dataset_name, table_name, table_name_2)
	#response body for api request
	# return f'Uploaded raw csv file to bucket: {0}, and appended data to BigQuery dataset: {1}, table: {2} on '.format(bucket_name,dataset_name,table_name) + _getToday() TODO: this will be the response from the api

if __name__ == "__main__":
		handler(schema_bq, schema_df)