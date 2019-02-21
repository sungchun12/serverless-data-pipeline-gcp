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

#%%
#define environment variables
project_id = 'iconic-range-220603' #capture the project id to where this data will land
bucket_name = 'chicago_traffic_raw' #capture bucket name where raw data will be stored
dataset_name = 'chicago_traffic_demo' #initial dataset
table_name = 'table_raw' #name of table to capture data
table_desc = 'Raw, public Chicago traffic data is appended to this table every 5 minutes'#table description
nulls_expected = ('_comments') #tuple of nulls expected for checking data outliers
partition_by = '_last_updt' #partition by the last updated field for faster querying and incremental loadsl
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


#%%
def _getToday():
    """Create timestamp string"""
    return datetime.now().strftime('%Y%m%d%H%M%S')


#%%
timestamp = _getToday()

timestamp


#%%
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


#%%
create_bucket(bucket_name)


#%%
def dataset_exists(client, dataset_reference):
    """Return if a table exists.

    Args:
        client (google.cloud.bigquery.client.Client):
            A client to connect to the BigQuery API.
        table_reference (google.cloud.bigquery.table.TableReference):
            A reference to the table to look for.

    Returns:
        bool: ``True`` if the table exists, ``False`` otherwise.
    """
    from google.cloud.exceptions import NotFound

    try:
        client.get_dataset(dataset_reference)
        return True
    except NotFound:
        return False


#%%
def table_exists(client, table_reference):
    """Return if a table exists.

    Args:
        client (google.cloud.bigquery.client.Client):
            A client to connect to the BigQuery API.
        table_reference (google.cloud.bigquery.table.TableReference):
            A reference to the table to look for.

    Returns:
        bool: ``True`` if the table exists, ``False`` otherwise.
    """
    from google.cloud.exceptions import NotFound

    try:
        client.get_table(table_reference)
        return True
    except NotFound:
        return False


#%%
#https://cloud.google.com/bigquery/docs/python-client-migration#update_a_table
def create_dataset_table(dataset_name, table_name, table_desc, schema, partition_by):
    """Detects whether or not a new dataset and/or table need to be created"""
    #setup the client
    bigquery_client = bigquery.Client()

    # Create a DatasetReference using a chosen dataset ID.
    dataset_ref = bigquery_client.dataset(dataset_name) # The project defaults to the Client's project if not specified.

    # Construct a full Dataset object to send to the API.
    dataset = bigquery.Dataset(dataset_ref)

    # Specify the geographic location where the dataset should reside.
    dataset.location = "US"
    
    # Send the dataset to the API for creation.
    # Raises google.api_core.exceptions.Conflict if the Dataset already
    # exists within the project.
    if dataset_exists(bigquery_client, dataset_ref) == False: #checks if dataset not found
        dataset = bigquery_client.create_dataset(dataset)  # API request
        print("Created new dataset")
    else:
        print("Dataset already exists")
    
    #Create an empty table
    table_ref = dataset_ref.table(table_name) #construct a full table object to send to the api
    
    if table_exists(bigquery_client, table_ref) == False: #checks if table not found
        table = bigquery.Table(table_ref, schema=schema)
        table.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY, #day is the only supported type for now
            field=partition_by)  # name of column to use for partitioning
        table = bigquery_client.create_table(table)  # API request
        assert table.table_id == table_name #checks if table_id matches table_name
        
        #update the table description
        table.description = table_desc
        table = bigquery_client.update_table(table, ['description'])  # API request
        assert table.description == table_desc #checks if table description matches the update
        print("Created empty table partitioned on column: {}".format(table.time_partitioning.field))
    else:
        print("Table already exists within dataset")


#%%
create_dataset_table(dataset_name, table_name, table_desc, schema_bq, partition_by)


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
results_df = create_results_df()


#%%
#function upload data to cloud storage
def upload_raw_data_gcs(results_df, bucket_name):
    """Upload dataframe into google cloud storage bucket"""
    try: 
        # Write the DataFrame to GCS (Google Cloud Storage)
        storage_client = storage.Client.from_service_account_json('service_account.json') #authenticate service account
        bucket = storage_client.bucket(bucket_name) #capture bucket details
        results_df.to_csv('traffic_' + _getToday() + '.csv') #convert dataframe to csv file type
        source_file_name = 'traffic_' + _getToday() + '.csv' #create the file name
        blob = bucket.blob(os.path.basename(source_file_name)) #define the path to the file
        blob.upload_from_filename(source_file_name) #upload to bucket
        print("Successfully uploaded csv file into {}".format(bucket))
    except Exception as e:
        print("Failure to upload data to google cloud storage bucket :(")
        raise e


#%%
upload_raw_data_gcs(results_df, bucket_name)


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


#%%
results_df_transformed = convert_schema(results_df, schema_df)

print(results_df_transformed.dtypes)


#%%
test_df = results_df_transformed.isnull().any()

test_df


#%%
#check if there are any nulls in the columns except for _comments

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


#%%
null_columns = check_nulls(results_df_transformed)


#%%
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
null_outliers = check_null_outliers(null_columns, nulls_expected)


#%%
#figure out the nullable vs. required mode schema mismatch
def upload_to_gbq(results_df_transformed, project_id, dataset_name, table_name):
    """Uploads data into bigquery and appends if data already exists"""
    try:
        gbq.to_gbq(results_df_transformed, dataset_name+"."+table_name, project_id, 
                   if_exists='append', location = 'US', progress_bar=False)
        print('Data uploaded into project: {0}, dataset: {1}, table: {2}'.format(project_id, dataset_name, table_name))
    except Exception as e:
        print("Failure to upload data to Bigquery :(")
        raise e


#%%
upload_to_gbq(results_df_transformed, project_id, dataset_name, table_name)


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
table_name_2 = "unique_records_staging"
query_unique_records(project_id, dataset_name, table_name, table_name_2)


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


#%%
table_name = 'unique_records_staging'
table_name_2 = 'unique_records_final'
append_unique_records(project_id, dataset_name, table_name, table_name_2)


#%%
# return f'Uploaded raw csv file to bucket: {0}, and appended data to BigQuery dataset: {1}, table: {2} on '.format(bucket_name,dataset_name,table_name) + _getToday()


