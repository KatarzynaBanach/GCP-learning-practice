variable "project" {}

variable "credentials_file" {}

variable "region" {
    default = "us-central1"
}

variable "zone" {
    default = "us-central1-a"
}

variable "location" {
    default = "US"
}

variable "storage_class" {
    default = "STANDARD"
}


# variable "bucket_params" {
#     type = object({
#         name = string
#         location = string
#         storage_class = string
#     })
#     description = "Bucket parameters"
#     default = {
#         name = "test-terraform-bucket"
#         location = "US"
#         storage_class = "STANDARD"
#     }
#     validation {
#         condition = length(var.bucket_params.name) > 3
#         error_message = "Bucket name must be at least 4 characters."
#     }
# }

variable "object_params" {
    type = object({
        name = string
        source = string
    })
    description = "Object parameters"
    default = {
        name = "customer_data.csv"
        source = "/mnt/c/Users/KatarzynaBanach/GCP-learning-practice/Beam/customer_data.csv"
    }
}