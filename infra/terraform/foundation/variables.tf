variable "project_id" {
  type        = string
  description = "GCP project that hosts the pipeline infrastructure."
  default     = "realtime-telemetry-gcp"
}

variable "region" {
  type        = string
  description = "Default region for regional resources (Frankfurt)."
  default     = "europe-west3"
}

variable "github_repository" {
  type        = string
  description = "owner/name of the GitHub repo allowed to deploy via Workload Identity Federation."
  default     = "theofanis-tsakanikas/realtime-telemetry-pipeline"
}

variable "state_bucket" {
  type        = string
  description = "Name of the (manually-seeded) GCS bucket holding Terraform state."
  default     = "realtime-telemetry-gcp-tfstate"
}

variable "bigquery_dataset" {
  type        = string
  description = "BigQuery dataset for the analytics sink (Spark → BigQuery, dbt marts)."
  default     = "telemetry"
}

variable "bigquery_location" {
  type        = string
  description = "BigQuery dataset location."
  default     = "europe-west3"
}
