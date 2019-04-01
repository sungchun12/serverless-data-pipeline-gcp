#!/usr/bin/env python
"""This contains the bigquery and dataframe schemas for data warehouse setup.

Change these values for your table schemas in scope.
Data types defined in BigQuery are mapped to pandas dataframe data types.
If data types do not match, data will not be able to be uploaded to bigquery.

"""
# gcp modules
from google.cloud import bigquery

# apply a schema during BigQuery table creation
schema_bq = [
    bigquery.SchemaField(
        "_comments",
        "STRING",
        mode="NULLABLE",
        description="Provides extra context to traffic segment",
    ),
    bigquery.SchemaField(
        "_direction",
        "STRING",
        mode="NULLABLE",
        description="Traffic flow direction for the segment.",
    ),
    bigquery.SchemaField(
        "_fromst",
        "STRING",
        mode="NULLABLE",
        description="Start street for the segment in the direction of traffic flow.",
    ),
    bigquery.SchemaField(
        "_last_updt",
        "TIMESTAMP",
        mode="NULLABLE",
        description=" If the \
          LAST_UPDATED time is several days old, it can be assumed that no \
          transit service over the segment currently. \
          These segments are included in the Chicago Traffic Tracker dataset \
          because they are key routes and CDOT intends to monitor \
          traffic conditions through other means in the near future. \
          Will display UTC, but truly represents CST.",
    ),  # in CST -06:00
    bigquery.SchemaField(
        "_length",
        "FLOAT",
        mode="NULLABLE",
        description="Length of the segment in miles.",
    ),
    bigquery.SchemaField(
        "_lif_lat",
        "FLOAT",
        mode="NULLABLE",
        description="The starting point latitude. \
          See start_lon for a fuller description.",
    ),
    bigquery.SchemaField(
        "_lit_lat",
        "FLOAT",
        mode="NULLABLE",
        description="The ending point latitude. \
          See start_lon for a fuller description.",
    ),
    bigquery.SchemaField(
        "_lit_lon",
        "FLOAT",
        mode="NULLABLE",
        description="The ending point longitude. \
          See start_lon for a fuller description.",
    ),
    bigquery.SchemaField(
        "_strheading",
        "STRING",
        mode="NULLABLE",
        description="The position of the segment in the address grid. \
          North, South, East, or West of State and Madison.",
    ),
    bigquery.SchemaField(
        "_tost",
        "STRING",
        mode="NULLABLE",
        description="End street for the segment in the direction of traffic flow.",
    ),
    bigquery.SchemaField(
        "_traffic",
        "INTEGER",
        mode="NULLABLE",
        description="Real-time estimated speed in miles per hour. \
          For congestion advisory and traffic maps, this value is compared to \
          a 0-9, 10-20, and 21 & over scale to display heavy, medium, \
          and free flow conditions for the traffic segment. \
          Except for a very few segments speed on city arterials is limited \
          to 30 mph by ordinance.",
    ),
    bigquery.SchemaField(
        "segmentid",
        "INTEGER",
        mode="NULLABLE",
        description="Unique arbitrary number to represent each segment.",
    ),
    bigquery.SchemaField(
        "start_lon",
        "FLOAT",
        mode="NULLABLE",
        description="The longitude associated with the starting point of the \
          segment in the direction of traffic flow. \
          For two-way streets it is roughly at the middle of the half \
            that the segment is representing. \
          For one-way streets this is the street center line. ",
    ),
    bigquery.SchemaField(
        "street",
        "STRING",
        mode="NULLABLE",
        description="Street name of the traffic segment",
    ),
]

# apply a schema to pandas dataframe to match BigQuery for equivalent types
schema_df = {
    "_comments": "object",
    "_direction": "object",
    "_fromst": "object",
    "_last_updt": "datetime64",
    "_length": "float64",
    "_lif_lat": "float64",
    "_lit_lat": "float64",
    "_lit_lon": "float64",
    "_strheading": "object",
    "_tost": "object",
    "_traffic": "int64",
    "segmentid": "int64",
    "start_lon": "float64",
    "street": "object",
}
