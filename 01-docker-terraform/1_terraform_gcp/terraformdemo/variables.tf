variable "credentials" {
  description = "My credentials"
  default     = "./keys/my-creds.json"
}

variable "project" {
  description = "Project"
  default     = "protean-music-448109-m2"
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
  description = "BigQuery dataset name"
  default     = "demo_dataset"
}

variable "gcs_bucket_name" {
  description = "My storage bucket name"
  default     = "protean-music-448109-m2-terra-bucket"
}
variable "gcs_storage_class" {
  description = "Storage class for GCS bucket"
  default     = "STANDARD"
}