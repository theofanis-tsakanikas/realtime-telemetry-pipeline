variable "project_id" {
  type        = string
  description = "GCP project that hosts the pipeline infrastructure."
  default     = "realtime-telemetry-gcp"
}

variable "region" {
  type        = string
  description = "Region for all regional resources (Frankfurt)."
  default     = "europe-west3"
}

variable "zone" {
  type        = string
  description = "Zone for the stack VM."
  default     = "europe-west3-a"
}

variable "machine_type" {
  type        = string
  description = "VM size. The full stack needs ~8-10GB RAM."
  default     = "e2-standard-4"
}

variable "boot_disk_size_gb" {
  type        = number
  description = "Boot disk size (room for Docker images)."
  default     = 50
}

variable "repo_url" {
  type        = string
  description = "Repo the VM clones (SSH, using the read-only deploy key)."
  default     = "git@github.com:theofanis-tsakanikas/realtime-telemetry-pipeline.git"
}

variable "repo_branch" {
  type        = string
  description = "Branch the VM checks out."
  default     = "main"
}

variable "auto_stop_minutes" {
  type        = number
  description = "Safety net: the VM powers off this many minutes after boot."
  default     = 480
}

# --- Foundation-provided identifiers (Layer 0 outputs). Stable/deterministic, so
# --- passed as variables instead of reading the foundation state — the app layer
# --- has zero dependency on (and no read access to) the foundation's state file.

variable "runtime_service_account_email" {
  type        = string
  description = "Runtime SA (created in the foundation) attached to the VM."
  default     = "telemetry-stack-vm@realtime-telemetry-gcp.iam.gserviceaccount.com"
}

variable "redis_secret" {
  type    = string
  default = "telemetry-redis-password"
}

variable "slack_secret" {
  type    = string
  default = "telemetry-slack-webhook-url"
}

variable "grafana_secret" {
  type    = string
  default = "telemetry-grafana-admin-password"
}

variable "deploy_secret" {
  type    = string
  default = "telemetry-deploy-key"
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
