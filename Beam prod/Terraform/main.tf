provider "google" {
    project = var.project
    credentials = var.credentials_file
    region = var.region
    zone = var.zone
}


resource "google_storage_bucket" "beam_stage" {
  name     = "beam-stage-1122"
  location = var.location
  storage_class = var.storage_class
}


resource "google_storage_bucket" "raw_customer_data" {
  name     = "raw-customer-data-1122"
  location = var.location
  storage_class = var.storage_class
}


resource "google_storage_bucket_object" "customer_data" {
    name = var.object_params.name
    source = var.object_params.source
    bucket = google_storage_bucket.raw_customer_data.name
}