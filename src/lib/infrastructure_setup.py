#!/usr/bin/env python
"""Module which creates data pipeline storage infrastructure

This module has functions that create a raw data bucket
in google cloud storage, and creates dataset-table pairs.

"""
# gcp modules
from google.cloud import storage
from google.cloud import bigquery

# import logging
from lib.helper_functions import set_logger

logger = set_logger(__name__)


def create_bucket(bucket_name):
    """Creates a bucket if not detected

    Args:
        bucket_name: name of GCS bucket to be created

    Returns:
        ``Created a new bucket: <bucket path>``
          OR ``Bucket already exists: <bucket path>``

    """
    client = storage.Client()
    # authenticate service account
    # .from_service_account_json('service_account.json')
    bucket = client.bucket(bucket_name)  # capture bucket details
    bucket.location = "US-CENTRAL1"  # define regional location
    if not bucket.exists():  # checks if bucket doesn't exist
        bucket.create()
        logger.info(f"Created a new bucket: {bucket.path}")
    else:
        logger.info(f"Bucket already exists: {bucket.path}")


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


# https://cloud.google.com/bigquery/docs/python-client-migration#update_a_table
def create_dataset_table(dataset_name, table_name, table_desc, schema, partition_by):
    """Creates a new dataset and/or table if not detected.

    Args:
        dataset_name: Name of dataset to be created
        table_name: Name of table to be created within dataset
        table_desc: table descriptions
        schema: table schema with data types
        partition_by: Which datetime field to partition by

    Returns:
        ``Created new dataset: <dataset path>``
          OR ``Dataset already exists: <dataset path>``
        &
        ``Created empty table partitioned on column: partition_by``
          OR ``Table already exists: <table path>``

    """
    # setup the client
    bigquery_client = bigquery.Client()

    # Create a DatasetReference using a chosen dataset ID.
    dataset_ref = bigquery_client.dataset(
        dataset_name
    )  # The project defaults to the Client's project if not specified.

    # Construct a full Dataset object to send to the API.
    dataset = bigquery.Dataset(dataset_ref)

    # Specify the geographic location where the dataset should reside.
    dataset.location = "US"

    # Send the dataset to the API for creation.
    # Raises google.api_core.exceptions.
    # Conflict if the Dataset already exists within the project.
    if (
        dataset_exists(bigquery_client, dataset_ref) is False
    ):  # checks if dataset not found
        dataset = bigquery_client.create_dataset(dataset)  # API request
        logger.info(f"Created new dataset: {dataset_ref.path}")
    else:
        logger.info(f"Dataset already exists: {dataset_ref.path}")

    # Create an empty table
    table_ref = dataset_ref.table(
        table_name
    )  # construct a full table object to send to the api

    if table_exists(bigquery_client, table_ref) is False:
        table = bigquery.Table(table_ref, schema=schema)
        table.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field=partition_by,  # day is the only supported type for now
        )  # name of column to use for partitioning
        table = bigquery_client.create_table(table)
        assert table.table_id == table_name  # checks if table_id matches

        # update the table description
        table.description = table_desc
        table = bigquery_client.update_table(table, ["description"])
        assert (
            table.description == table_desc
        )  # checks if table description matches the update
        logger.info(
            f"Created empty table partitioned \
              on column: {table.time_partitioning.field}"
        )
    else:
        logger.info(f"Table already exists: {table_ref.path}")
