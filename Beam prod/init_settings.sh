BUCKET_NAME=temp_beam_location_1

gcloud services disable dataflow.googleapis.com --force
gcloud services enable dataflow.googleapis.com

gcloud storage buckets create gs://$BUCKET_NAME --location=EU  # delete it and change to 1 -> terraform would be ok
gcloud storage cp customer_data.csv gs://$BUCKET_NAME/customer/customer_data.csv

# todo:
# terraform for bucket
# safe fileds countre writetotext into gcs
