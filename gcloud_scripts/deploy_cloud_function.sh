
#explain directory structure: https://cloud.google.com/functions/docs/writing/#structuring_source_code
#tutorial for local cloud function deployment: https://cloud.google.com/functions/docs/deploying/filesystem

#deploy function
gcloud functions deploy test-sung --entry-point handler --runtime python37 --trigger-topic test_topic

#publish message to pubsub topic trigger
gcloud pubsub topics publish test_topic --message "Can you see this?"

#check logs
gcloud functions logs read --limit 20