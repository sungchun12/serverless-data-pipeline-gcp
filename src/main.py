#!/usr/bin/env python
"""Cloud Function which creates an end to end data pipeline.

API Data Source: https://dev.socrata.com/foundry/data.cityofchicago.org/n4j6-wkkf

This Cloud Function is responsible for:
-Tracing performance of subsets of function calls via spans
-Defining and creating infrastructure such as dataset, tables, bucket
-Ingesting raw data from an api call into google cloud storage
-Converting a pandas dataframe raw data schema to match BigQuery
-Ingesting data into BigQuery
-Run SQL queries capturing recent and unique records based on current date of invocation
-Appending unique records to final table

"""
# decoding module for pubsub
import base64

# opencensus modules to trace function performance
# https://opencensus.io/exporters/supported-exporters/python/stackdriver/
import opencensus
from opencensus.trace import tracer as tracer_module
from opencensus.trace.exporters import stackdriver_exporter
from opencensus.trace.exporters.transports.background_thread import (
    BackgroundThreadTransport,
)

# lib modules
from lib.bq_api_data_functions import (
    append_unique_records,
    bq_table_num_rows,
    query_unique_records,
)
from lib.data_ingestion import (
    check_null_outliers,
    check_nulls,
    convert_schema,
    create_results_df,
    upload_raw_data_gcs,
    upload_to_gbq,
)
from lib.helper_functions import set_logger
from lib.infrastructure_setup import create_bucket, create_dataset_table

logger = set_logger(__name__)


# explains why to use pubsub as middleware
# https://cloud.google.com/scheduler/docs/start-and-stop-compute-engine-instances-on-a-schedule
def handler(event, context):
    """Entry point function that orchestrates the data pipeline from start to finish.

    Triggered from a message on a Cloud Pub/Sub topic.

    Args:
        event (dict): Event payload.
        context (google.cloud.functions.Context): Metadata for the event.

    """
    # instantiate trace exporter
    project_id = (
        "iconic-range-220603"
    )  # capture the project id to where this data will land
    exporter = stackdriver_exporter.StackdriverExporter(
        project_id=project_id, transport=BackgroundThreadTransport
    )
    # instantiate tracer
    tracer = tracer_module.Tracer(exporter=exporter)

    with tracer.span(name="get_kpis") as span_get_kpis:
        # prints a message from the pubsub trigger
        pubsub_message = base64.b64decode(event["data"]).decode("utf-8")
        print(pubsub_message)  # can be used to configure dynamic pipeline

        with span_get_kpis.span(name="infrastructure_var_setup"):
            # define infrastructure variables
            bucket_name = (
                "chicago_traffic_raw"
            )  # capture bucket name where raw data will be stored
            dataset_name = "chicago_traffic_demo"  # initial dataset
            table_raw = "traffic_raw"  # name of table to capture data
            table_desc = "Raw, public Chicago traffic data is appended \
                to this table every 5 minutes"  # table description
            table_staging = "traffic_staging"
            table_staging_desc = f"Unique records greater than or equal to \
                current date from table: {table_raw}"
            table_final = "traffic_final"
            table_final_desc = f"Unique, historical records \
                accumulated from table: {table_raw}"
            nulls_expected = (
                "_comments"
            )  # tuple of nulls expected for checking data outliers
            partition_by = (
                "_last_updt"
            )  # partition by the last updated field for faster querying
            # and incremental loads
            from lib.schemas import schema_bq, schema_df  # import schemas

        with span_get_kpis.span(name="infrastructure_creation") as span_infra_create:
            # create infrastructure
            with span_infra_create.span(name="bucket_creation"):
                create_bucket(bucket_name)
            with span_infra_create.span(name="dataset_table_creation"):
                create_dataset_table(
                    dataset_name, table_raw, table_desc, schema_bq, partition_by
                )  # create raw table
                create_dataset_table(
                    dataset_name,
                    table_staging,
                    table_staging_desc,
                    schema_bq,
                    partition_by,
                )  # create a table for unique records staging
                create_dataset_table(
                    dataset_name, table_final, table_final_desc, schema_bq, partition_by
                )  # create a table for unique records final

        with span_get_kpis.span(name="ingest_raw_data") as span_ingest_raw:
            with span_ingest_raw.span(name="create_dataframe"):
                # access data from API, create dataframe, and upload raw csv
                results_df = create_results_df()
            with span_ingest_raw.span(name="upload_raw_data_gcs"):
                upload_raw_data_gcs(results_df, bucket_name)

        with span_get_kpis.span(name="convert_schema"):
            # perform schema conversion on dataframe to match bigquery schema
            results_df_transformed = convert_schema(results_df, schema_df)
            print(results_df_transformed.dtypes)

        with span_get_kpis.span(name="audit_null_columns"):
            # check if there are any nulls in the columns and print exceptions
            null_columns = check_nulls(results_df_transformed)
            null_outliers = check_null_outliers(null_columns, nulls_expected)

        with span_get_kpis.span(name="upload_to_gbq"):
            # upload data to bigquery
            upload_to_gbq(results_df_transformed, project_id, dataset_name, table_raw)
            bq_table_num_rows(dataset_name, table_raw)

        with span_get_kpis.span(name="preprocess_data") as span_prep_data:
            # Preprocess data for unique records accumulation
            with span_prep_data.span(name="query_unique_records"):
                query_unique_records(project_id, dataset_name, table_raw, table_staging)
                bq_table_num_rows(dataset_name, table_staging)
            with span_prep_data.span(name="append_unique_records"):
                append_unique_records(
                    project_id, dataset_name, table_staging, table_final
                )
                bq_table_num_rows(dataset_name, table_final)
        logger.info("Data Pipeline Fully Realized!")
