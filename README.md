# serverless_data_pipeline_gcp

**Deploy an end to end data pipeline for Chicago traffic api data using: Cloud Functions, Pub/Sub, Cloud Storage, Cloud Scheduler, BigQuery**

**Use Cases:**

* Use this as a template for your data pipelines
* Use this to extract and land Chicago traffic data
* Pick and choose which modules/code snippets are useful
* Show me how to do it better :)

**Technical Concepts:**


**Technologies:** Cloud Shell, Cloud Functions, Pub/Sub, Cloud Storage, Cloud Scheduler, BigQuery

**Languages:** Python 3.7, SQL(Standard)

**Further Reading:** For those looking for production-level deployments

* Streaming Tutorial: https://cloud.google.com/solutions/streaming-data-from-cloud-storage-into-bigquery-using-cloud-functions#create_the_cloud_storage_bucket
* BQPipeline Utility Functions: https://github.com/GoogleCloudPlatform/professional-services/tree/master/tools/bqpipeline
* I discovered the above after I created this pipeline...HA!


# """

# Add description here

# # make sure to install these packages before running:

# # these should all be in the requirements.txt file when deploying cloud function

# # pip install pandas

# # pip install sodapy

# # pip install google-cloud-storage

# # pip install google-cloud-bigquery

# # pip install pandas-gbq

# # pip install gcloud

# # pip install pyarrow

# #this is needed for local development in order to appropriately access GCP from a local service account key

# #https://cloud.google.com/docs/authentication/production#auth-cloud-compute-engine-python

# #may not use this at all and have cloud functions use the default service account: https://cloud.google.com/docs/authentication/production#auth-cloud-compute-engine-python

# # os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="./service_account.json", used for jupyter notebook

# # https://cloud.google.com/docs/authentication/getting-started#auth-cloud-implicit-python

# #run the below in windomws command prompt for authentication

# # set GOOGLE_APPLICATION_CREDENTIALS=C:\Users\sungwon.chung\Desktop\cloud_function_poc\service_account.json

# #for linux: export GOOGLE_APPLICATION_CREDENTIALS="mnt/c/Users/sungwon.chung/Desktop/cloud_function_poc/service_account.json"

# """
