# --------------------------------------------------------------------------- #
# Secret CONTAINERS only — values are NEVER in Terraform code or state. You push
# them from your local .env with `make cloud-secrets` (push-secrets.sh), and the
# VM reads them at boot. Keeps secrets out of git AND out of tfstate.
# --------------------------------------------------------------------------- #

locals {
  secret_ids = {
    redis_password         = "telemetry-redis-password"
    slack_webhook_url      = "telemetry-slack-webhook-url"
    grafana_admin_password = "telemetry-grafana-admin-password"
    deploy_key             = "telemetry-deploy-key" # read-only SSH key to clone the private repo
  }
}

resource "google_secret_manager_secret" "this" {
  for_each = local.secret_ids

  secret_id = each.value
  replication {
    auto {}
  }
  depends_on = [google_project_service.required]
}

# The VM's runtime service account may read each secret's value.
resource "google_secret_manager_secret_iam_member" "vm_access" {
  for_each = google_secret_manager_secret.this

  secret_id = each.value.id
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:${google_service_account.vm.email}"
}
