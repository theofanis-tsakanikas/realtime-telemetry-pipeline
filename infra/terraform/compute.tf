# --------------------------------------------------------------------------- #
# The stack VM. On boot, the startup script installs Docker, clones the repo,
# pulls the secrets from Secret Manager, writes infra/.env, and runs the full
# Docker Compose stack. No external IP — reachable only via IAP.
# --------------------------------------------------------------------------- #

resource "google_service_account" "vm" {
  account_id   = "telemetry-stack-vm"
  display_name = "Runtime SA for the telemetry stack VM"
}

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
    # No access_config block => no external IP (IAP-only).
  }

  service_account {
    email  = google_service_account.vm.email
    scopes = ["cloud-platform"] # actual access is gated by IAM (secretAccessor)
  }

  metadata = {
    enable-oslogin = "TRUE"
    startup-script = templatefile("${path.module}/startup.sh.tftpl", {
      project_id        = var.project_id
      repo_url          = var.repo_url
      repo_branch       = var.repo_branch
      auto_stop_minutes = var.auto_stop_minutes
      redis_secret      = local.secret_ids.redis_password
      slack_secret      = local.secret_ids.slack_webhook_url
      grafana_secret    = local.secret_ids.grafana_admin_password
      deploy_secret     = local.secret_ids.deploy_key
    })
  }

  scheduling {
    automatic_restart = false # a guest-initiated auto-stop should stay stopped
    preemptible       = false
  }

  # Allow stop/start (pause/resume) without recreating the VM.
  allow_stopping_for_update = true

  depends_on = [
    google_project_service.required,
    google_compute_router_nat.nat,
    google_secret_manager_secret.this,
  ]
}
