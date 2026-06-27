# --------------------------------------------------------------------------- #
# GitHub Actions → GCP via Workload Identity Federation (no long-lived keys).
#
# GitHub presents a signed OIDC token; GCP trusts this pool/provider and lets the
# workflow impersonate the deployer service account — but ONLY for tokens whose
# `repository` claim equals THIS repo (the attribute_condition). This mirrors the
# AWS OIDC deployer role in the first project, on the second cloud.
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

# The identity GitHub Actions impersonates to run Terraform.
resource "google_service_account" "deployer" {
  project      = var.project_id
  account_id   = "gha-deployer"
  display_name = "GitHub Actions Terraform deployer"
}

# Allow workflows from this repo to impersonate the deployer SA.
resource "google_service_account_iam_member" "wif_user" {
  service_account_id = google_service_account.deployer.name
  role               = "roles/iam.workloadIdentityUser"
  member             = "principalSet://iam.googleapis.com/${google_iam_workload_identity_pool.github.name}/attribute.repository/${var.github_repository}"
}

# Permissions the deployer needs to manage this config (Redis VM in Phase 2).
# Scoped to what the stack actually creates; tighten further per-resource later.
locals {
  # Minimal roles for the deployer to manage the app infra (VM/network/secrets) on a
  # manual `apply` dispatch. The CI *PR* check never assumes this SA — it only runs
  # fmt + validate with no backend, so it needs no cloud access at all (no escalation).
  # Granting broader IAM/WIF admin (for a CI-driven apply of the foundation itself) is a
  # deliberate, owner-only step, intentionally kept out of the automated path.
  deployer_roles = [
    "roles/compute.admin",          # the VM, VPC, firewall, NAT, IAP
    "roles/secretmanager.admin",    # the Redis/Slack/Grafana/deploy-key secrets
    "roles/storage.admin",          # read/write the remote state bucket
    "roles/iam.serviceAccountUser", # attach a service account to the VM
  ]
}

resource "google_project_iam_member" "deployer" {
  for_each = toset(local.deployer_roles)

  project = var.project_id
  role    = each.value
  member  = "serviceAccount:${google_service_account.deployer.email}"
}
