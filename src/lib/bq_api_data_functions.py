#!/usr/bin/env python
"""
Add a description here
"""
import logging, sys

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

formatter = logging.Formatter('%(levelname)s:%(asctime)s:%(name)s:%(message)s')

stream_handler = logging.StreamHandler(sys.stdout)
stream_handler.setFormatter(formatter)

logger.addHandler(stream_handler)

#gcp modules
from google.cloud import bigquery

def bq_table_num_rows(dataset_name, table_name):
		"""Print total number of rows in destination bigquery table"""
		bigquery_client = bigquery.Client() #instantiate bigquery client to interact with api
		dataset_ref = bigquery_client.dataset(dataset_name) #create dataset object
		table_ref = dataset_ref.table(table_name) #create table object 
		table = bigquery_client.get_table(table_ref)  # API Request
		num_rows = table.num_rows
		logger.info(f"Total number of rows: {table.num_rows} in {table_ref.path}")
		return num_rows

def query_max_timestamp(project_id, dataset_name, table_name):
		"""Return the max timestamp from a table in bigquery after current date in CST"""
		#in central Chicago time
		sql = f"SELECT max(_last_updt) as max_timestamp FROM `{project_id}.{dataset_name}.{table_name}` WHERE _last_updt >= TIMESTAMP(CURRENT_DATE('-06:00'))"
		bigquery_client = bigquery.Client() #setup the client
		query_job = bigquery_client.query(sql) #run the query
		results = query_job.result() # waits for job to complete
		for row in results: #returns the result
				max_timestamp = row.max_timestamp.strftime('%Y-%m-%d %H:%M:%S')
				return max_timestamp

# https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs#methods
# https://googleapis.github.io/google-cloud-python/latest/bigquery/generated/google.cloud.bigquery.job.QueryJobConfig.html#google.cloud.bigquery.job.QueryJobConfig
# The following values are supported: 
# configuration.copy.writeDisposition
# WRITE_TRUNCATE: If the table already exists, BigQuery overwrites the table data. 
# WRITE_APPEND: If the table already exists, BigQuery appends the data to the table. 
# WRITE_EMPTY: If the table already exists and contains data, a 'duplicate' error is returned in the job result. 
def query_unique_records(project_id, dataset_name, table_name, table_name_2):
		"""Queries unique records from original table and saves results in staging table"""
		bigquery_client = bigquery.Client()
		job_config = bigquery.QueryJobConfig()
		table_ref = bigquery_client.dataset(dataset_name).table(table_name_2) #set destination table
		job_config.destination = table_ref
		job_config.write_disposition = 'WRITE_TRUNCATE'
		max_timestamp = query_max_timestamp(project_id, dataset_name, table_name)
		sql = f"SELECT * FROM `{project_id}.{dataset_name}.{table_name}` WHERE _last_updt >= TIMESTAMP(DATETIME '{max_timestamp}');"
		query_job = bigquery_client.query(
				sql,
				# Location must match that of the dataset(s) referenced in the query
				# and of the destination table.
				location='US',
				job_config=job_config)  # API request - starts the query
		query_job.result()
		logger.info(f"Query results loaded to table {table_ref.path}")

def append_unique_records(project_id, dataset_name, table_name, table_name_2):
		"""Queries unique staging table and appends new results onto final table"""
		bigquery_client = bigquery.Client()
		job_config = bigquery.QueryJobConfig()
		table_ref = bigquery_client.dataset(dataset_name).table(table_name_2) #set destination table
		job_config.destination = table_ref
		job_config.write_disposition = 'WRITE_APPEND'
		sql = f"SELECT a.* FROM `{project_id}.{dataset_name}.{table_name}` a LEFT JOIN `{project_id}.{dataset_name}.{table_name_2}` b on a.segmentid = b.segmentid AND a._last_updt = b._last_updt WHERE b.segmentid IS NULL;"
		query_job = bigquery_client.query(
				sql,
				# Location must match that of the dataset(s) referenced in the query
				# and of the destination table.
				location='US',
				job_config=job_config)  # API request - starts the query
		query_job.result()
		logger.info(f"Query results loaded to table {table_ref.path}")
