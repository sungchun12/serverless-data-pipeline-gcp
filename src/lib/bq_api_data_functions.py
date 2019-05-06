#!/usr/bin/env python
"""Module which captures number of rows in tables and runs SQL queries.

This module is responsible for:
-Capturing total number of rows in a BigQuery table
-Querying max timestamp in a date field based on rows within current date
-Querying unique records based on the above timestamp
-Appending unique records to a final table

"""
# gcp modules
from google.cloud import bigquery

# import logging
from lib.helper_functions import set_logger

logger = set_logger(__name__)


def bq_table_num_rows(dataset_name, table_name):
    """Log total number of rows in destination bigquery table.

    Args:
        dataset_name: destination dataset name
        table_name: destination table name

    Returns:
        Integer object: ``num_rows``

    """
    bigquery_client = (
        bigquery.Client()
    )  # instantiate bigquery client to interact with api
    dataset_ref = bigquery_client.dataset(dataset_name)  # create dataset obj
    table_ref = dataset_ref.table(table_name)  # create table obj
    table = bigquery_client.get_table(table_ref)  # API Request
    num_rows = table.num_rows
    logger.info(f"Total number of rows: {table.num_rows} in {table_ref.path}")
    return num_rows


def query_max_timestamp(project_id, dataset_name, table_name):
    """Return the max timestamp in BigQuery table after current date in CST.

    Args:
        project_id: destination project id
        dataset_name: destination dataset name
        table_name: destination table name

    Returns:
        string object: ``max_timestamp``

    """
    # in central Chicago time
    sql = f"SELECT max(_last_updt) as max_timestamp \
            FROM `{project_id}.{dataset_name}.{table_name}` \
            WHERE _last_updt >= TIMESTAMP(CURRENT_DATE('-06:00'))"
    bigquery_client = bigquery.Client()  # setup the client
    query_job = bigquery_client.query(sql)  # run the query
    results = query_job.result()  # waits for job to complete
    for row in results:  # returns the result
        max_timestamp = row.max_timestamp.strftime("%Y-%m-%d %H:%M:%S")
        return max_timestamp


# https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs#methods
# https://googleapis.github.io/google-cloud-python/latest/bigquery/generated/google.cloud.bigquery.job.QueryJobConfig.html#google.cloud.bigquery.job.QueryJobConfig
# The following values are supported:
# configuration.copy.writeDisposition
# WRITE_TRUNCATE: If the table already exists, BigQuery overwrites the table
# WRITE_APPEND: If the table already exists, BigQuery appends the data
# WRITE_EMPTY: If the table already exists and contains data,
# a 'duplicate' error is returned in the job result.
def query_unique_records(project_id, dataset_name, table_name, table_name_2):
    """Queries unique records from original table, saves in staging table.

    Unique records are filtered by filtering all records >= the max timestamp.

    Args:
        project_id: destination project id
        dataset_name: destination dataset name
        table_name: starting table name
        table_name_2: destination table name

    """
    bigquery_client = bigquery.Client()
    job_config = bigquery.QueryJobConfig()
    table_ref = bigquery_client.dataset(dataset_name).table(
        table_name_2
    )  # set destination table
    job_config.destination = table_ref
    job_config.write_disposition = "WRITE_TRUNCATE"
    max_timestamp = query_max_timestamp(project_id, dataset_name, table_name)
    sql = f"SELECT DISTINCT * FROM `{project_id}.{dataset_name}.{table_name}` \
            WHERE _last_updt >= TIMESTAMP(DATETIME '{max_timestamp}');"
    query_job = bigquery_client.query(
        sql,
        # Location must match that of the dataset(s) referenced in the query
        # and of the destination table.
        location="US",
        job_config=job_config,
    )  # API request - starts the query
    query_job.result()
    logger.info(f"Query results loaded to table {table_ref.path}")


def append_unique_records(project_id, dataset_name, table_name, table_name_2):
    """Queries unique staging table and appends new results onto final table.

    Args:
        project_id: destination project id
        dataset_name: destination dataset name
        table_name: starting table name
        table_name_2: destination table name

    """
    bigquery_client = bigquery.Client()
    job_config = bigquery.QueryJobConfig()
    table_ref = bigquery_client.dataset(dataset_name).table(
        table_name_2
    )  # set destination table
    job_config.destination = table_ref
    job_config.write_disposition = "WRITE_APPEND"
    # left outer join to avoid appending duplicate data
    sql = f"SELECT a.* FROM `{project_id}.{dataset_name}.{table_name}` a \
            LEFT JOIN `{project_id}.{dataset_name}.{table_name_2}` b \
            on a.segmentid = b.segmentid AND a._last_updt = b._last_updt \
            WHERE b.segmentid IS NULL;"
    query_job = bigquery_client.query(
        sql,
        # Location must match that of the dataset(s) referenced in the query
        # and of the destination table.
        location="US",
        job_config=job_config,
    )  # API request - starts the query
    query_job.result()
    logger.info(f"Query results loaded to table {table_ref.path}")
