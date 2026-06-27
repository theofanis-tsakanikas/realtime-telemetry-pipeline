variable "project_id" {
  type        = string
  description = "GCP project that hosts the pipeline infrastructure."
  default     = "realtime-telemetry-gcp"
}

variable "region" {
  type        = string
  description = "Region for all regional resources (Frankfurt, low latency from Greece)."
  default     = "europe-west3"
}

variable "zone" {
  type        = string
  description = "Zone for the stack VM."
  default     = "europe-west3-a"
}

variable "github_repository" {
  type        = string
  description = "owner/name of the GitHub repo allowed to deploy via Workload Identity Federation."
  default     = "theofanis-tsakanikas/realtime-telemetry-pipeline"
}

variable "machine_type" {
  type        = string
  description = "VM size. The full stack (Kafka+Spark+Redis+Grafana+...) needs ~8-10GB RAM."
  default     = "e2-standard-4" # 4 vCPU / 16 GB, ~$0.13/hr — destroyed/paused when idle
}

variable "boot_disk_size_gb" {
  type        = number
  description = "Boot disk size (room for Docker images: Spark, Kafka, redis-stack, etc.)."
  default     = 50
}

variable "repo_url" {
  type        = string
  description = "Repo the VM clones (SSH, using the read-only deploy key — works with a private repo)."
  default     = "git@github.com:theofanis-tsakanikas/realtime-telemetry-pipeline.git"
}

variable "repo_branch" {
  type        = string
  description = "Branch the VM checks out."
  default     = "main"
}

variable "auto_stop_minutes" {
  type        = number
  description = "Safety net: the VM powers itself off (-> TERMINATED, no compute charge) this many minutes after boot, in case you forget to pause it."
  default     = 480 # 8 hours
}
