# --------------------------------------------------------------------------- #
# GitHub Actions → GCP via Workload Identity Federation (no long-lived keys).
# Lives in the FOUNDATION layer: the deployer identity is never managed by the
# app layer it deploys, so CI can apply/destroy the app layer without touching
# (or being able to destroy) its own auth foundation.
# --------------------------------------------------------------------------- #

resource "google_iam_workload_identity_pool" "github" {
  project                   = var.project_id
  workload_identity_pool_id = "github-pool"
  display_name              = "GitHub Actions"
  description               = "Federated identity pool for GitHub Actions OIDC."

  depends_on = [google_project_service.required]
}

resource "google_iam_workload_identity_pool_provider" "github" {
  project                            = var.project_id
  workload_identity_pool_id          = google_iam_workload_identity_pool.github.workload_identity_pool_id
  workload_identity_pool_provider_id = "github-provider"
  display_name                       = "GitHub OIDC"

  attribute_mapping = {
    "google.subject"       = "assertion.sub"
    "attribute.repository" = "assertion.repository"
  }

  # Hard security boundary: only tokens from this exact repo are accepted.
  attribute_condition = "assertion.repository == '${var.github_repository}'"

  oidc {
    issuer_uri = "https://token.actions.githubusercontent.com"
  }
}

# The identity GitHub Actions impersonates to deploy the APP layer.
resource "google_service_account" "deployer" {
  project      = var.project_id
  account_id   = "gha-deployer"
  display_name = "GitHub Actions Terraform deployer (app layer)"
}

resource "google_service_account_iam_member" "wif_user" {
  service_account_id = google_service_account.deployer.name
  role               = "roles/iam.workloadIdentityUser"
  member             = "principalSet://iam.googleapis.com/${google_iam_workload_identity_pool.github.name}/attribute.repository/${var.github_repository}"
}

# Scoped roles — exactly what the APP layer needs, nothing more. No IAM/WIF admin,
# no secret admin (secret access is granted to the runtime SA here in the foundation),
# so the deployer can never escalate or manage its own foundation.
locals {
  deployer_project_roles = [
    "roles/compute.admin",           # app VPC / firewall / NAT
    "roles/iam.serviceAccountUser",  # act as the runtime / node SAs
    "roles/container.admin",         # create/delete the GKE Autopilot cluster
    "roles/artifactregistry.writer", # CI pushes the stack images to the registry
  ]
}

resource "google_project_iam_member" "deployer" {
  for_each = toset(local.deployer_project_roles)

  project = var.project_id
  role    = each.value
  member  = "serviceAccount:${google_service_account.deployer.email}"
}

# State access scoped to the state bucket only (read foundation outputs are not
# needed — the app layer takes foundation identifiers as inputs — but the deployer
# must read/write the app-layer state object + acquire locks).
resource "google_storage_bucket_iam_member" "deployer_state" {
  bucket = var.state_bucket
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.deployer.email}"
}
