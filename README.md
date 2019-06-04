# Serverless Data Pipeline GCP

**Deploy an end to end data pipeline for Chicago traffic api data and measure function performance using: Cloud Functions, Pub/Sub, Cloud Storage, Cloud Scheduler, BigQuery, Stackdriver Trace**

**Usage:**

- Use this as a template for your data pipelines
- Use this to extract and land Chicago traffic data
- Pick and choose which modules/code snippets are useful
- Understand how to measure function performance throughout execution
- Show me how to do it better :)

**Data Pipeline Operations:**

1. Creates raw data bucket
2. Creates BigQuery dataset and raw, staging, and final tables with defined schemas
3. Downloads data from Chicago traffic API
4. Ingests data as pandas dataframe
5. Uploads pandas dataframe to raw data bucket as a parquet file
6. Converts dataframe schema to match BigQuery defined schema
7. Uploads pandas dataframe to raw BigQuery table
8. Run SQL queries to capture and accumulate unique records based on current date
9. Sends function performance metrics to Stackdriver Trace

**Technologies:** Cloud Shell, Cloud Functions, Pub/Sub, Cloud Storage, Cloud Scheduler, BigQuery, Stackdriver Trace

**Languages:** Python 3.7, SQL(Standard)

**Technical Concepts:**

- This was designed to be a python native solution and starter template for simple data pipelines
- This follows the procedural paradigm which groups like-functions in separate files and imports them as modules into the main entrypoint file. Each operation goes through ordered actions on objects vs. OOP in which objects perform the actions
- Pub/Sub is used as middleware as opposed to invoking an HTTP-triggered cloud function to minimize overhead code securing the URL in addition to Pub/Sub serving as a shock absorber to a sudden burst in invocations
- This is not intended for robust production deployment as it doesn't account for edge cases and dynamic error handling
- Lessons Learned: Leverage classes for interdependent functions and creating extensibility in table objects. I also forced stacktracing functionality to measure function performance on a pub/sub trigger, so you can't create a report based on http requests to analyze performance trends. Stackdriver Trace will start auto-creating performance reports once there's enough data.

**Further Reading:** For those looking for production-level deployments

- Streaming Tutorial: <https://cloud.google.com/solutions/streaming-data-from-cloud-storage-into-bigquery-using-cloud-functions#create_the_cloud_storage_bucket>
- BQPipeline Utility Functions: <https://github.com/GoogleCloudPlatform/professional-services/tree/master/tools/bqpipeline>
- I discovered the above after I created this pipeline...HA!

## Deployment Instructions

**Prerequisites:**

- An open Google Cloud account: <https://cloud.google.com/free/>
- Proficient in Python and SQL
- A heart and mind eager to create data pipelines

[![Open in Cloud Shell](http://gstatic.com/cloudssh/images/open-btn.png)](https://console.cloud.google.com/cloudshell/editor?cloudshell_git_repo=https://github.com/sungchun12/serverless_data_pipeline_gcp.git)

_OR_

1.  Activate Cloud Shell: <https://cloud.google.com/shell/docs/quickstart#start_cloud_shell>
2.  Clone repository

```bash
git clone https://github.com/sungchun12/serverless_data_pipeline_gcp.git
```

- Enable Google Cloud APIs: Stackdriver Trace API, Cloud Functions API, Cloud Pub/Sub API, Cloud Scheduler API, Cloud Storage, BigQuery API, Cloud Build API(gcloud CLI equivalent below when submitted through cloud shell)

```bash
#set project id
gcloud config set project [your-project-id]
```

```bash
#Enable Google service APIs
gcloud services enable \
    cloudfunctions.googleapis.com \
    cloudtrace.googleapis.com \
    pubsub.googleapis.com \
    cloudscheduler.googleapis.com \
    storage-component.googleapis.com \
    bigquery-json.googleapis.com \
    cloudbuild.googleapis.com
```

---

**Note**: If you want to automate the build and deployment of this pipeline, submit the commands in order in cloud shell after completing the above steps. It skips step 5 below as it is redundant for auto-deployment.

Find your Cloudbuild service account in the IAM console. Ex: [unique-id]@cloudbuild.gserviceaccount.com

```bash
#add role permissions to CloudBuild Service Account
gcloud projects add-iam-policy-binding [your-project-id] \
    --member serviceAccount:[unique-id]@cloudbuild.gserviceaccount.com \
    --role roles/cloudfunctions.developer \
    --role roles/cloudscheduler.admin \
    --role roles/logging.viewer
```

```bash
#Deploy steps in cloudbuild configuration file
gcloud builds submit --config cloudbuild.yaml .
```

---

3.  Change directory to relevant code

```bash
cd serverless_data_pipeline_gcp/src
```

4.  Deploy cloud function with pub/sub trigger. Note: this will automatically create the trigger if it does not exist

```bash
gcloud functions deploy [function-name] --entry-point handler --runtime python37 --trigger-topic [topic-name]
```

Ex:

```bash
gcloud functions deploy demo_function --entry-point handler --runtime python37 --trigger-topic demo_topic
```

5.  Test cloud function by publishing a message to pub/sub topic

```bash
gcloud pubsub topics publish [topic-name] --message "<your-message>"
```

Ex:

```bash
gcloud pubsub topics publish demo_topic --message "Can you see this?"
```

6.  Check logs to see how function performed. You may have to re-execute this command line multiple times if logs don't show up initially

```bash
gcloud functions logs read --limit 50
```

7.  Deploy cloud scheduler job which publishes a message to Pub/Sub every 5 minutes

```bash
gcloud beta scheduler jobs create pubsub [job-name] \
    --schedule "*/5 * * * *" \
    --topic [topic-name] \
    --message-body '{"<Message-to-publish-to-pubsub>"}' \
    --time-zone 'America/Chicago'
```

Ex:

```bash
gcloud beta scheduler jobs create pubsub schedule_function \
    --schedule "*/5 * * * *" \
    --topic demo_topic \
    --message-body '{"Can you see this? With love, cloud scheduler"}' \
    --time-zone 'America/Chicago'
```

8.  Test end to end pipeline by manually running cloud scheduler job. Next, repeat step 6 above.

```bash
gcloud beta scheduler jobs run [job-name]
```

Ex:

```bash
gcloud beta scheduler jobs run schedule_function
```

9.  Understand pipeline performance by opening stacktrace and click "get_kpis": <https://cloud.google.com/trace/docs/quickstart#view_the_trace_overview>

**YOUR PIPELINE IS DEPLOYED AND MEASURABLE!**

**Note:** You'll notice extraneous blocks of comments and commented out code throughout the python scripts.
