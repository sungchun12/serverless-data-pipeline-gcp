#%% Change working directory from the workspace root to the ipynb file location. Turn this addition off with the DataScience.changeDirOnImportExport setting
import os
try:
	os.chdir(os.path.join(os.getcwd(), '.ipynb_checkpoints'))
	print(os.getcwd())
except:
	pass

#%%
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
from sodapy import Socrata
import pandas_gbq as gbq
from datetime import datetime
from gcloud import storage


#%%
#this is needed for local development in order to appropriately access GCP from a local service account key
#https://cloud.google.com/docs/authentication/production#auth-cloud-compute-engine-python
#may not use this at all and have cloud functions use the default service account
os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="./service_account.json"


#%%
#define environment variables
project_id = 'iconic-range-220603' #capture the project id to where this data will land
bucket_name = 'demo_sung_v2' #capture bucket name where raw data will be stored
dataset_name = 'demo_sung_v2' #initial dataset
table_name = 'demo_sung_v4' #name of table to capture data
table_desc = 'Raw, public Chicago traffic data is appended to this table every 5 minutes'#table description
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
def create_dataset_table(dataset_name, table_name, table_desc, schema):
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
        table = bigquery_client.create_table(table)  # API request
        assert table.table_id == table_name #checks if table_id matches table_name
        
        #update the table description
        table.description = table_desc
        table = bigquery_client.update_table(table, ['description'])  # API request
        assert table.description == table_desc #checks if table description matches the update
        print("Created empty table")
    else:
        print("Table already exists within dataset")            


#%%
create_dataset_table(dataset_name, table_name, table_desc, schema_bq)


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
        print("Total number of rows: {0} in dataset: {1}, table:{2}".format(table.num_rows, dataset_name, table_name))
        return num_rows
    except Exception as e:
        print("Failure to count number of rows in dataset: {0}, table:{1}".format(dataset_name, table_name))
        raise e


#%%
bq_table_num_rows(dataset_name, table_name)


#%%
return f'Uploaded raw csv file to bucket: {0}, and appended data to BigQuery dataset: {1}, table: {2} on '.format(bucket_name,dataset_name,table_name) + _getToday()


