"""
Add a description here
"""

#gcp modules
from google.cloud import storage
from google.cloud import bigquery

#TODO: create a method where a service account is explicitly 
# authorized to create a cloud function vs. using my local service account file
def create_bucket(bucket_name):
    """Detects whether or not a new bucket needs to be created"""
    client = storage.Client()
		# .from_service_account_json('service_account.json') #authenticate service account
    bucket = client.bucket(bucket_name) #capture bucket details
    bucket.location = 'US-CENTRAL1' #define regional location
    if not bucket.exists(): #checks if bucket doesn't exist
        bucket.create()
        print("Created a new bucket: {0}".format(bucket_name))
    else:
        print("Bucket already exists")


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
    """
		Detects whether or not a new dataset and/or table need to be created.
		Creates the dataset and table if either do not exist.
		"""
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