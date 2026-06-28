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

variable "control_plane_authorized_cidrs" {
  type = list(object({
    cidr_block   = string
    display_name = string
  }))
  description = <<-EOT
    CIDRs allowed to reach the GKE control-plane (public) endpoint. Defaults to
    open so kubectl works out of the box — NARROW THIS to your IP/32 before
    applying. CI deploys via Connect Gateway (not this endpoint), so locking it
    down does not break the deploy workflow.
  EOT
  default = [
    {
      cidr_block   = "0.0.0.0/0"
      display_name = "open — narrow to your IP/32"
    }
  ]
}
