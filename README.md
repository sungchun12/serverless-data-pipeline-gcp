# Serverless Data Pipeline GCP

**Deploy an end to end data pipeline for Chicago traffic api data using: Cloud Functions, Pub/Sub, Cloud Storage, Cloud Scheduler, BigQuery**

**Usage:**

- Use this as a template for your data pipelines
- Use this to extract and land Chicago traffic data
- Pick and choose which modules/code snippets are useful
- Show me how to do it better :)

**Technologies:** Cloud Shell, Cloud Functions, Pub/Sub, Cloud Storage, Cloud Scheduler, BigQuery

**Languages:** Python 3.7, SQL(Standard)

**Technical Concepts:**

- This was designed to be a python native solution and starter template for simple data pipelines
- This follows the procedural paradigm which groups like-functions in separate files and imports them as modules into the main entrypoint file. Each operation goes through ordered actions on objects vs. OOP in which objects perform the actions
- This is not intended for robust production deployment as it doesn't account for edge cases and dynamic error handling
- Lessons Learned: Leverage classes for interdependent functions and creating extensibility in table objects. I also forced stacktracing functionality to measure function performance on a pub/sub trigger, so you can't create a report to analyze performance trends.

**Further Reading:** For those looking for production-level deployments

- Streaming Tutorial: https://cloud.google.com/solutions/streaming-data-from-cloud-storage-into-bigquery-using-cloud-functions#create_the_cloud_storage_bucket
- BQPipeline Utility Functions: https://github.com/GoogleCloudPlatform/professional-services/tree/master/tools/bqpipeline
- I discovered the above after I created this pipeline...HA!

# Deployment Instructions

**Prerequisites:**

- An open Google Cloud account: https://cloud.google.com/free/
- Proficient in Python and SQL
- A heart and mind eager to create data pipelines
- Enable Google Cloud APIs: Stackdriver Trace API, Cloud Functions API, Cloud Pub/Sub API, Cloud Scheduler API, Cloud Storage, BigQuery API

1. Activate Cloud Shell
2. Clone repository

```git clone https://github.com/sungchun12/serverless_data_pipeline_gcp.git```

3. Change directory to relevant code

```cd serverless_data_pipeline_gcp/src```

4. Deploy cloud function with pub/sub trigger. Note: this will automatically create the trigger if it does not exist




# this is needed for local development in order to appropriately access GCP from a local service account key

# #https://cloud.google.com/docs/authentication/production#auth-cloud-compute-engine-python

# #may not use this at all and have cloud functions use the default service account: https://cloud.google.com/docs/authentication/production#auth-cloud-compute-engine-python

# # os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="./service_account.json", used for jupyter notebook

# # https://cloud.google.com/docs/authentication/getting-started#auth-cloud-implicit-python

# #run the below in windomws command prompt for authentication

# # set GOOGLE_APPLICATION_CREDENTIALS=C:\Users\sungwon.chung\Desktop\cloud_function_poc\service_account.json

# #for linux: export GOOGLE_APPLICATION_CREDENTIALS="mnt/c/Users/sungwon.chung/Desktop/cloud_function_poc/service_account.json"

# """
