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


#%%
#get record count in raw table
# from google.cloud import bigquery
# client = bigquery.Client()
# dataset_id = 'my_dataset'
# table_id = 'my_table'
def bq_table_num_rows(dataset_name, table_name):
    """Print total number of rows in destination bigquery table"""
    try:
        bigquery_client = bigquery.Client() #instantiate bigquery client to interact with api
        dataset_ref = bigquery_client.dataset(dataset_name) #create dataset object
        table_ref = dataset_ref.table(table_name) #create table object 
        table = bigquery_client.get_table(table_ref)  # API Request
        num_rows = table.num_rows
        print("Total number of rows: {0} in {1}".format(table.num_rows, table_ref.path))
        return num_rows
    except Exception as e:
        print("Failure to count number of rows in dataset: {0}, table: {1}".format(dataset_name, table_name))
        raise e


#%%
#may not be possible to use table parameters in queries
#think of something new
#what if I define it as a formatted string first and then format it as a docstring using concatenates? It works
#the result should return in CST
def query_max_timestamp(project_id, dataset_name, table_name):
    """Return the max timestamp from a table in BigQuery after current date in CST"""
    #in central Chicago time
    sql = "SELECT max(_last_updt) as max_timestamp FROM `{0}.{1}.{2}` WHERE _last_updt >= TIMESTAMP(CURRENT_DATE('-06:00'))"    .format(project_id, dataset_name, table_name)
    
    bigquery_client = bigquery.Client() #setup the client
    
    query_job = bigquery_client.query(sql) #run the query
    
    results = query_job.result() # waits for job to complete
    for row in results:
        max_timestamp = row.max_timestamp.strftime('%Y-%m-%d %H:%M:%S')
        return max_timestamp

#%%
#query unique records and append to unique records table
#Figure out how to query a table with python and append results
#-it works only in first try into empty table and then errors out if table already exists
#for this query to append, I need a union statement
#this function can only workf or the first time
#CURRENT_DATE('-06:00')
# https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs#methods
# https://googleapis.github.io/google-cloud-python/latest/bigquery/generated/google.cloud.bigquery.job.QueryJobConfig.html#google.cloud.bigquery.job.QueryJobConfig
# The following values are supported: 
# configuration.copy.writeDisposition
# WRITE_TRUNCATE: If the table already exists, BigQuery overwrites the table data. 
# WRITE_APPEND: If the table already exists, BigQuery appends the data to the table. 
# WRITE_EMPTY: If the table already exists and contains data, a 'duplicate' error is returned in the job result. 
def query_unique_records(project_id, dataset_name, table_name, table_name_2):
    """Queries unique records from original table and saves results in staging table"""
    try:
        bigquery_client = bigquery.Client()
        job_config = bigquery.QueryJobConfig()
        table_ref = bigquery_client.dataset(dataset_name).table(table_name_2) #set destination table
        job_config.destination = table_ref
        job_config.write_disposition = 'WRITE_TRUNCATE'
        max_timestamp = query_max_timestamp(project_id, dataset_name, table_name)
        sql = "SELECT * FROM `{0}.{1}.{2}` WHERE _last_updt >= TIMESTAMP(DATETIME '{3}');"        .format(project_id, dataset_name, table_name, max_timestamp)
        query_job = bigquery_client.query(
            sql,
            # Location must match that of the dataset(s) referenced in the query
            # and of the destination table.
            location='US',
            job_config=job_config)  # API request - starts the query
        query_job.result()
        print('Query results loaded to table {}'.format(table_ref.path))
    except Exception as e:
        raise e

#%%
def append_unique_records(project_id, dataset_name, table_name, table_name_2):
    """Queries unique staging table and appends new results onto final table"""
    try:
        bigquery_client = bigquery.Client()
        job_config = bigquery.QueryJobConfig()
        table_ref = bigquery_client.dataset(dataset_name).table(table_name_2) #set destination table
        job_config.destination = table_ref
        job_config.write_disposition = 'WRITE_APPEND'
        sql = "SELECT a.* FROM `{0}.{1}.{2}` a LEFT JOIN `{0}.{1}.{3}` b on a.segmentid = b.segmentid AND a._last_updt = b._last_updt WHERE b.segmentid IS NULL AND b._last_updt IS NULL;"        .format(project_id, dataset_name, table_name, table_name_2)
        query_job = bigquery_client.query(
            sql,
            # Location must match that of the dataset(s) referenced in the query
            # and of the destination table.
            location='US',
            job_config=job_config)  # API request - starts the query
        query_job.result()
        print('Query results loaded to table {}'.format(table_ref.path))
    except Exception as e:
        raise e