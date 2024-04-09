BUCKET_NAME=temp_beam_location_1

gcloud services disable dataflow.googleapis.com --force
gcloud services enable dataflow.googleapis.com

gcloud storage buckets create gs://$BUCKET_NAME --location=EU  # delete it and change to 1 -> terraform would be ok
gcloud storage cp customer_data.csv gs://$BUCKET_NAME/customer/customer_data.csv

# todo:
# terraform for bucket
# add if _main_
# maybe function in case of transformation for fb and pinterest? then list of sources?
# vm to do it, instead of cloud shell
# inform it there is a new source
# podzielić na pliki, bilbioteki itp. osobno ta część z tworzeniem datasetu
# w plikach można dać więcej statystyk odnośnie tych dzielonych tabel / spróbować połączyć je w jeden plik.


# challanges:
# difference between running locally and in dataflow eg. variables
