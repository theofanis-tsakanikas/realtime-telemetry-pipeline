terraform {
  required_version = ">= 1.5"

  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 6.0"
    }
  }

  # Layer 0 (foundation) state. Separate prefix from the app layer so the two have
  # independent lifecycles. The bucket is the manually-created seed (kept out of
  # Terraform — it must never be able to destroy its own state store).
  backend "gcs" {
    bucket = "realtime-telemetry-gcp-tfstate"
    prefix = "foundation"
  }
}
