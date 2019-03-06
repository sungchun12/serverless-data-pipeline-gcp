"""
Add a description here
"""

#%%
#built in python modules
import os
#gcp modules
from google.cloud import storage
import pandas_gbq as gbq
from google.cloud import bigquery
#api module
from sodapy import Socrata
#pandas dataframe module
import pandas as pd

from lib.helper_functions import _getToday

#%%
#create pandas dataframe
def create_results_df():
    """Create a dataframe based on JSON from the Chicago traffic API"""
    try:
        # First 2000 results, returned as JSON from API / converted to Python list of
        # dictionaries by sodapy.
        # Unauthenticated client only works with public data sets. Note 'None'
        # in place of application token, and no username or password:
        data_client = Socrata("data.cityofchicago.org", None)
        results = data_client.get("8v9j-bter", limit=2000) #unique id for chicago traffic data

        # Convert to pandas DataFrame
        results_df = pd.DataFrame.from_records(results)
        print("Successfully created a pandas dataframe!")
        
        return results_df
    except Exception as e:
        print("Failure to create a pandas dataframe :(")
        raise e

#%%
#function upload data to cloud storage
#GZIP compression uses more CPU resources than Snappy or LZO, but provides a higher compression ratio. 
# GZip is often a good choice for cold data, which is accessed infrequently. 
# Snappy or LZO are a better choice for hot data, which is accessed frequently.
# The only writeable part of the filesystem is the /tmp directory, which you can use to store temporary files in a function instance. 
# This is a local disk mount point known as a "tmpfs" volume in which data written to the volume is stored in memory. 
# Note that it will consume memory resources provisioned for the function.
#TODO: fix the write to tmp directory
#right now, it's erroring out because it's trying to write the parquet file to the default read only system
def upload_raw_data_gcs(results_df, bucket_name):
	"""Upload dataframe into google cloud storage bucket"""
	try: 
		# Write the DataFrame to GCS (Google Cloud Storage)
		storage_client = storage.Client()
		# .from_service_account_json('service_account.json') #authenticate service account
		bucket = storage_client.bucket(bucket_name) #capture bucket details
		timestamp = _getToday()
		source_file_name = 'traffic_'+ timestamp + '.gzip' #create the file name
		temp_path = "/tmp"
		os.chdir(temp_path) #change to tmp path
		# blob.upload_from_string(results_df.to_parquet(source_file_name, engine = 'pyarrow', compression = 'gzip'),content_type='gzip')
		results_df.to_parquet(source_file_name, engine = 'pyarrow', compression = 'gzip')
		# blob = bucket.blob(os.path.basename(source_file_name)) #define the path to the file
		blob = bucket.blob(source_file_name) #define the binary large object(blob)
		blob.upload_from_filename(source_file_name) #upload to bucket
		print("Successfully uploaded parquet gzip file into {}".format(bucket))
		#delete files from temporary folder
		#TODO: create separate function for this
		for file in os.listdir(temp_path):
			file_path = os.path.join(temp_path, file)
			if os.path.isfile(file_path):
				os.unlink(file_path)
	except Exception as e:
		print("Failure to upload data to google cloud storage bucket :(")
		raise e

#%%
#https://stackoverflow.com/questions/21886742/convert-pandas-dtypes-to-bigquery-type-representation
#https://stackoverflow.com/questions/44953463/pandas-google-bigquery-schema-mismatch-makes-the-upload-fail
#http://pbpython.com/pandas_dtypes.html
    
def convert_schema(results_df, schema_dict):
	"""Converts data types in dataframe to match BigQuery destination table"""
	for k, v in schema_dict.items(): #for each column name in the dictionary, convert the data type in the dataframe
		results_df[k] = results_df[k].astype(v)
	results_df_transformed = results_df
	print("Updated schema to match BigQuery destination table")
	return results_df_transformed

def check_nulls(results_df_transformed):
	"""Checks if there are any nulls in the columns"""
	try:
		null_columns = []
		check_bool = results_df_transformed.isnull().any() #returns a boolean True/False for every column in dataframe if it contains nulls
		for k, v in check_bool.items(): #for each column in check_bool having nulls, print the name of the column
			if check_bool[v] == False:
				null_columns.append(k)
		print("These are the null columns: {}".format(null_columns))
		return null_columns
	except Exception as e:
		print(e)

def check_null_outliers(null_columns, nulls_expected):
    """Checks if there are any outlier nulls in the columns"""
    try:
        null_outliers=[] #empty list to collect list of columns that are not expected to be null
        #if any columns in the nulls expected mismatch the nulls collected, append to null_outliers
        if any(x not in nulls_expected for x in null_columns): 
          	null_outliers.append(x)
        print("These are the outlier null columns: {}".format(null_outliers))
        return null_outliers
    except Exception as e:
        print(e)

#%%
#figure out the nullable vs. required mode schema mismatch
#TODO: refactor to use bigquery instead of pandas-gbq api as it should upload faster
#https://cloud.google.com/bigquery/docs/pandas-gbq-migration#loading_a_pandas_dataframe_to_a_table
#https://github.com/pydata/pandas-gbq/issues/133#issuecomment-411119426
def upload_to_gbq(results_df_transformed, project_id, dataset_name, table_name):
	"""Uploads data into bigquery and appends if data already exists"""
	try:
		bigquery_client = bigquery.Client()
		dataset_ref = bigquery_client.dataset(dataset_name)
		table_ref = dataset_ref.table(table_name)
		# job_config = bigquery.job.LoadJobConfig() #configure how the data loads into bigquery
		# job_config.write_disposition = 'WRITE_TRUNCATE' #if table exists, append to it
		# job_config.ignoreUnknownValues = 'T' #ignore columns that don't match destination schema
		# job_config.schema_update_options ='ALLOW_FIELD_ADDITION'
		# #TODO: bad request due to schema mismatch with an index field
		# bigquery_client.load_table_from_dataframe(results_df_transformed, table_ref, num_retries = 5, job_config = job_config).result() 
		gbq.to_gbq(results_df_transformed, dataset_name+"."+table_name, project_id, 
								if_exists='append', location = 'US', progress_bar=True)
		print('Data uploaded into: {}'.format(table_ref.path))
	except Exception as e:
		print("Failure to upload data to Bigquery :(")
		raise e