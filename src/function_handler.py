"""
Add description here
"""

#%% Change working directory from the workspace root to the ipynb file location. Turn this addition off with the DataScience.changeDirOnImportExport setting
import os
try:
	os.chdir(os.path.join(os.getcwd(), 'src'))
	print(os.getcwd())
except:
	pass

#%%
#!/usr/bin/env python

# make sure to install these packages before running:
# these should all be in the requirements.txt file when deploying cloud function
# pip install pandas
# pip install sodapy
# pip install google-cloud-storage
# pip install google-cloud-bigquery
# pip install pandas-gbq
# pip install gcloud

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


#%%
#this is needed for local development in order to appropriately access GCP from a local service account key
#https://cloud.google.com/docs/authentication/production#auth-cloud-compute-engine-python
#may not use this at all and have cloud functions use the default service account: https://cloud.google.com/docs/authentication/production#auth-cloud-compute-engine-python
# os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="./service_account.json", used for jupyter notebook
# https://cloud.google.com/docs/authentication/getting-started#auth-cloud-implicit-python

#run the below in windomws command prompt for authentication
# set GOOGLE_APPLICATION_CREDENTIALS="C:\Users\sungwon.chung\Desktop\cloud_function_poc\src\service_account.json"

schema_bq = [
    bigquery.SchemaField('_comments', 'STRING', mode='NULLABLE'),
    bigquery.SchemaField('_direction', 'STRING', mode='NULLABLE'),
    bigquery.SchemaField('_fromst', 'STRING', mode='NULLABLE'),
    bigquery.SchemaField('_last_updt', 'TIMESTAMP', mode='NULLABLE'),
    bigquery.SchemaField('_length', 'FLOAT', mode='NULLABLE'),
    bigquery.SchemaField('_lif_lat', 'FLOAT', mode='NULLABLE'),
    bigquery.SchemaField('_lit_lat', 'FLOAT', mode='NULLABLE'),
    bigquery.SchemaField('_lit_lon', 'FLOAT', mode='NULLABLE'),
    bigquery.SchemaField('_strheading', 'STRING', mode='NULLABLE'),
    bigquery.SchemaField('_tost', 'STRING', mode='NULLABLE'),
    bigquery.SchemaField('_traffic', 'INTEGER', mode='NULLABLE'),
    bigquery.SchemaField('segmentid', 'INTEGER', mode='NULLABLE'),
    bigquery.SchemaField('start_lon', 'FLOAT', mode='NULLABLE'),
    bigquery.SchemaField('street', 'STRING', mode='NULLABLE')
] #apply a schema during BigQuery table creation

schema_df = {'_comments': 'object', 
               '_direction': 'object', 
               '_fromst': 'object', 
               '_last_updt': 'datetime64',          
               '_length': 'float64', 
               '_lif_lat': 'float64', 
               '_lit_lat': 'float64', 
               '_lit_lon': 'float64',
               '_strheading': 'object', 
               '_tost': 'object', 
               '_traffic': 'int64', 
               'segmentid': 'int64', 
               'start_lon': 'float64', 
               'street': 'object'
              } #apply a schema to pandas dataframe to match BigQuery for equivalent data types

#may want to define bigquery client, dataset_ref, and table_ref earlier in the handler function to avoid redundant code


def handler():
	#define environment variables
	project_id = 'iconic-range-220603' #capture the project id to where this data will land
	bucket_name = 'chicago_traffic_raw' #capture bucket name where raw data will be stored
	dataset_name = 'chicago_traffic_demo' #initial dataset
	table_name = 'table_raw' #name of table to capture data
	table_desc = 'Raw, public Chicago traffic data is appended to this table every 5 minutes'#table description
	nulls_expected = ('_comments') #tuple of nulls expected for checking data outliers
	partition_by = '_last_updt' #partition by the last updated field for faster querying and incremental loads




#%%
timestamp = _getToday()



#%%



#%%
create_bucket(bucket_name)




#%%
create_dataset_table(dataset_name, table_name, table_desc, schema_bq, partition_by)




#%%
results_df = create_results_df()





#%%
upload_raw_data_gcs(results_df, bucket_name)





#%%
results_df_transformed = convert_schema(results_df, schema_df)

print(results_df_transformed.dtypes)


#%%
test_df = results_df_transformed.isnull().any()

test_df


#%%
#check if there are any nulls in the columns except for _comments




#%%
null_columns = check_nulls(results_df_transformed)


#%%



#%%
null_outliers = check_null_outliers(null_columns, nulls_expected)





#%%
upload_to_gbq(results_df_transformed, project_id, dataset_name, table_name)





#%%
bq_table_num_rows(dataset_name, table_name)


#%%
# Create a query within cloud function to remove duplicates 
#in another destination dataset/table, incremental load based on date
# https://stackoverflow.com/questions/51475252/can-i-use-a-query-parameter-in-a-table-name
#order of operations:
#1. Get max timestamp from the raw table where timestamp>=today()
#2. Select * from raw table > max timestamp and append to unique record table
#3. Capture bq total number of rows



    

#%%
#check if it works
query_max_timestamp(project_id, dataset_name, table_name)

#%%
#create a table for unique records staging
table_name_2 = 'unique_records_staging'
table_desc_2 = "Unique records from table: {}".format(table_name)
create_dataset_table(dataset_name, table_name_2, table_desc_2, schema_bq, partition_by)


#%%
#create a table for unique records final
table_name_2 = 'unique_records_final'
table_desc_2 = "Unique records from table: {}".format(table_name)
create_dataset_table(dataset_name, table_name_2, table_desc_2, schema_bq, partition_by)





#%%
table_name_2 = "unique_records_staging"
query_unique_records(project_id, dataset_name, table_name, table_name_2)





#%%
table_name = 'unique_records_staging'
table_name_2 = 'unique_records_final'
append_unique_records(project_id, dataset_name, table_name, table_name_2)


#%%
# return f'Uploaded raw csv file to bucket: {0}, and appended data to BigQuery dataset: {1}, table: {2} on '.format(bucket_name,dataset_name,table_name) + _getToday()


