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

variable "cluster_name" {
  type        = string
  description = "GKE Autopilot cluster name (the stack runtime)."
  default     = "telemetry-autopilot"
}
