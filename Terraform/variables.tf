variable "project" {}

variable "credentials_file" {}

variable "region" {
    default = "us-central1"
}

variable "zone" {
    default = "us-central1-a"
}

variable "os_image" {
    default = "debian-cloud/debian-10"
}

variable "vm_params" {
    type = object({
        name = string
        machine_type = string
        zone = string
        allow_stopping_for_update = bool
    })
    description = "VM parameters"
    default = {
        name = "my-instance"
        machine_type = "f1-micro"
        zone = "us-central1-a"
        allow_stopping_for_update = true
    }
    validation {
        condition = length(var.vm_params.name) > 3
        error_message = "VM must be at least 4 characters."
    }
}
