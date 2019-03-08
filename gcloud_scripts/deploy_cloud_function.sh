
#explain directory structure: https://cloud.google.com/functions/docs/writing/#structuring_source_code
#tutorial for local cloud function deployment: https://cloud.google.com/functions/docs/deploying/filesystem
#all of this is run within cloud shell environment for linux/bash compatibility

#deploy function and creates a pub/sub topic if it doesn't exist
gcloud functions deploy test-sung --entry-point handler --runtime python37 --trigger-topic test_topic

#publish message to pubsub topic trigger to test function
gcloud pubsub topics publish test_topic --message "Can you see this?"

#check logs
gcloud functions logs read --limit 50

#deploy cloud scheduler job to run every 5 minutes to test functio
gcloud beta scheduler jobs create pubsub scheduled-function \
	--schedule "*/5 * * * *" \
	--topic test_topic \
	--message-body '{"Can you see this? With love, cloud scheduler"}' \
	--time-zone 'America/Chicago'

#manually run cloud scheduler job
gcloud beta scheduler jobs run scheduled-function