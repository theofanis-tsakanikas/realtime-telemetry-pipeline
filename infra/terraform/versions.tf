terraform {
  required_version = ">= 1.5"

  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 6.0"
    }
  }

  # Remote state in the manually-created "seed" bucket (see README). The bucket is
  # kept OUT of Terraform on purpose: Terraform must never be able to destroy the
  # store that holds its own state. The bucket name is an identifier, not a secret.
  backend "gcs" {
    bucket = "realtime-telemetry-gcp-tfstate"
    prefix = "infra"
  }
}
