provider "google" {
    project = var.project
    credentials = var.credentials_file
    region = var.region
    zone = var.zone
}

resource "google_compute_instance" "test_instance" {
    name = var.vm_params.name
    machine_type = var.vm_params.machine_type
    zone = var.vm_params.zone
    allow_stopping_for_update = var.vm_params.allow_stopping_for_update


    boot_disk {
        initialize_params {
            image = var.os_image
        }
    }


    network_interface {
        network = "default"
        access_config{
            //necessary even empty
        }
    }
}

resource "google_storage_bucket" "test_bucket" {
  name     = var.bucket_params.name
  location = var.bucket_params.location
  storage_class = var.bucket_params.storage_class
}


resource "google_storage_bucket_object" "test_object" {
    name = var.object_params.name
    source = var.object_params.source
    bucket = google_storage_bucket.test_bucket.name
}