variable "credentials" {
  description = "My Credentials"
  default     = "./.gc/ny-rides.json"
}

variable "project" {
  description = "project id"
  default     = "ny-rides-tyler-411718"
}

variable "region" {
  description = "Region"
  default     = "us-central1"
}

variable "location" {
  description = "Project Location"
  default     = "US"
}

variable "bq_dataset_name" {
  description = "My BigQuery dataset name"
  default     = "ny_rides-tyler-411718-dataset"
}

variable "gcs_bucket_name" {
  description = "My Storage Bucket Name"
  default     = "ny-rides-tyler-test-bucket"
}

variable "gcs_storage_class" {
  description = "Bucket Storage Class"
  default     = "STANDARD"
}