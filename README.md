# schedule_cloud_function
schedule a Google Cloud function to input data into BigQuery using cloud scheduler 
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
# # set GOOGLE_APPLICATION_CREDENTIALS=C:\Users\sungwon.chung\Desktop\cloud_function_poc\src\service_account.json
# #for linux: export GOOGLE_APPLICATION_CREDENTIALS="mnt/c/Users/sungwon.chung/Desktop/cloud_function_poc/src/service_account.json"

# """