# --------------------------------------------------------------------------- #
# Secrets + the VM runtime identity live in the FOUNDATION (seed) layer, so they
# PERSIST across app-layer destroy/apply cycles. Values are seeded ONCE (out of
# band, via `make cloud-seed-secrets`) — never in Terraform code or state. The app
# layer never touches secrets; it just attaches the runtime SA, which can read them.
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

# Runtime identity for the app VM (created here so the app layer only needs
# serviceAccountUser to attach it — not serviceAccountAdmin to create it).
resource "google_service_account" "vm" {
  project      = var.project_id
  account_id   = "telemetry-stack-vm"
  display_name = "Runtime SA for the telemetry stack VM"
}

# The runtime SA may read each secret's value (at VM boot).
resource "google_secret_manager_secret_iam_member" "vm_access" {
  for_each = google_secret_manager_secret.this

  secret_id = each.value.id
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:${google_service_account.vm.email}"
}
