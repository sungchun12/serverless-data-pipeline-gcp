"""
Add a description here
"""
#gcp modules
from google.cloud import bigquery




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