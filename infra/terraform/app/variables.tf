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
    CIDRs allowed to reach the GKE control-plane (public) endpoint. Locked to the
    owner's IP — CI deploys via Connect Gateway (not this endpoint), so this does
    not affect the deploy workflow. If your ISP rotates your IP and kubectl from
    your laptop starts timing out, update this /32 (curl ifconfig.me) and re-apply.
  EOT
  default = [
    {
      cidr_block   = "85.73.242.100/32"
      display_name = "owner laptop"
    }
  ]
}
