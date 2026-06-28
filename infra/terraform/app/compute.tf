# --------------------------------------------------------------------------- #
# The stack VM. Attaches the foundation's runtime SA (which can read the secrets);
# the app layer itself never creates identities or touches secret IAM. On boot the
# startup script installs Docker, clones the repo, fetches secrets, and runs the
# full stack. No external IP — reachable only via IAP.
# --------------------------------------------------------------------------- #

resource "google_compute_instance" "stack" {
  name         = "telemetry-stack"
  machine_type = var.machine_type
  zone         = var.zone
  tags         = ["telemetry-stack"]

  boot_disk {
    initialize_params {
      image = "debian-cloud/debian-12"
      size  = var.boot_disk_size_gb
      type  = "pd-balanced"
    }
  }

  network_interface {
    subnetwork = google_compute_subnetwork.subnet.id
    # No access_config => no external IP (IAP-only).
  }

  service_account {
    email  = var.runtime_service_account_email # created in the foundation layer
    scopes = ["cloud-platform"]
  }

  metadata = {
    enable-oslogin = "TRUE"
    startup-script = templatefile("${path.module}/startup.sh.tftpl", {
      project_id        = var.project_id
      repo_url          = var.repo_url
      repo_branch       = var.repo_branch
      auto_stop_minutes = var.auto_stop_minutes
      redis_secret      = var.redis_secret
      slack_secret      = var.slack_secret
      grafana_secret    = var.grafana_secret
      deploy_secret     = var.deploy_secret
    })
  }

  scheduling {
    automatic_restart = false
    preemptible       = false
  }

  allow_stopping_for_update = true

  depends_on = [google_compute_router_nat.nat]
}
