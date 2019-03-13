#!/usr/bin/env python
"""
add description here

"""
#opencensus modules to trace function performance
#https://opencensus.io/exporters/supported-exporters/python/stackdriver/
import opencensus
from opencensus.trace import tracer as tracer_module
from opencensus.trace.exporters import stackdriver_exporter
from opencensus.trace.exporters.transports.background_thread \
    import BackgroundThreadTransport

#decoding module for pubsub
import base64

#lib modules
from lib.bq_api_data_functions import bq_table_num_rows, query_unique_records, append_unique_records
from lib.data_ingestion import create_results_df, upload_raw_data_gcs, convert_schema, check_nulls, check_null_outliers, upload_to_gbq
from lib.infrastructure_setup import create_bucket, create_dataset_table
#TODO: add in logging: https://inventwithpython.com/blog/2012/04/06/stop-using-print-for-debugging-a-5-minute-quickstart-guide-to-pythons-logging-module/

#may want to define bigquery client, dataset_ref, and table_ref earlier in the handler function to avoid redundant code
#explains why to use pubsub as middleware https://cloud.google.com/scheduler/docs/start-and-stop-compute-engine-instances-on-a-schedule
def handler(event, context):
		"""
		Main function that orchestrates the data pipeline from start to finish.
		Triggered from a message on a Cloud Pub/Sub topic.

		Args:
				event (dict): Event payload.
				context (google.cloud.functions.Context): Metadata for the event.
		"""
		# instantiate trace exporter
		project_id = 'iconic-range-220603' #capture the project id to where this data will land
		exporter = stackdriver_exporter.StackdriverExporter(
    		project_id=project_id, transport=BackgroundThreadTransport)
		# instantiate tracer
		tracer = tracer_module.Tracer(exporter=exporter)
		with tracer.span(name='get_kpis') as span_get_kpis:
				#prints a message from the pubsub trigger
				pubsub_message = base64.b64decode(event['data']).decode('utf-8')
				print(pubsub_message) #can be used to configure dynamic pipeline creation
		with span_get_kpis.span(name='infrastructure_var_setup'):
				#TODO: create a class for all these objects? yes
				#https://dbader.org/blog/6-things-youre-missing-out-on-by-never-using-classes-in-your-python-code
				#define infrastructure variables
				bucket_name = 'chicago_traffic_raw' #capture bucket name where raw data will be stored
				dataset_name = 'chicago_traffic_demo' #initial dataset
				table_name = 'traffic_raw' #name of table to capture data
				table_desc = 'Raw, public Chicago traffic data is appended to this table every 5 minutes'#table description
				table_staging = 'traffic_staging'
				table_staging_desc = "Unique records greater than or equal to current date from table: {}".format(table_name)
				table_final = 'traffic_final'
				table_final_desc = "Unique, historical records accumulated from table: {}".format(table_name)
				nulls_expected = ('_comments') #tuple of nulls expected for checking data outliers
				partition_by = '_last_updt' #partition by the last updated field for faster querying and incremental loads
				from lib.schemas import schema_bq, schema_df #import schemas
		with span_get_kpis.span(name='infrastructure_creation') as span_infra_create:
				#create infrastructure
				with span_infra_create.span(name='bucket_creation'):
						create_bucket(bucket_name)
				with span_infra_create.span(name='dataset_table_creation'):
						create_dataset_table(dataset_name, table_name, table_desc, schema_bq, partition_by) #create raw table
						create_dataset_table(dataset_name, table_staging, table_staging_desc, schema_bq, partition_by) #create a table for unique records staging
						create_dataset_table(dataset_name, table_final, table_final_desc, schema_bq, partition_by) #create a table for unique records final

				#access data from API, create dataframe, and upload raw csv
				results_df = create_results_df()
				upload_raw_data_gcs(results_df, bucket_name)

				#perform schema conversion on dataframe to match bigquery schema
				results_df_transformed = convert_schema(results_df, schema_df)
				print(results_df_transformed.dtypes)

				#check if there are any nulls in the columns and print exceptions
				null_columns = check_nulls(results_df_transformed)
				null_outliers = check_null_outliers(null_columns, nulls_expected)

				#upload data to bigquery
				upload_to_gbq(results_df_transformed, project_id, dataset_name, table_name)
				bq_table_num_rows(dataset_name, table_name)

				#Preprocess data for unique records accumulation
				query_unique_records(project_id, dataset_name, table_name, table_staging)
				bq_table_num_rows(dataset_name, table_staging)
				append_unique_records(project_id, dataset_name, table_staging, table_final)
				bq_table_num_rows(dataset_name, table_final)
				print("Data Pipeline Fully Realized!")