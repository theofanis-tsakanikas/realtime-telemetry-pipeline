terraform {
  required_version = ">= 1.5"

  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 6.0"
    }
  }

  # Layer 1 (app) state — separate prefix from the foundation. This is the layer
  # CI deploys/destroys routinely; the foundation (identity, secrets) stays put.
  backend "gcs" {
    bucket = "realtime-telemetry-gcp-tfstate"
    prefix = "app"
  }
}
