# Identifiers consumed by the app layer (as variable defaults) and the GitHub
# Actions workflow. None are secrets.

output "workload_identity_provider" {
  description = "Full provider resource name for google-github-actions/auth."
  value       = google_iam_workload_identity_pool_provider.github.name
}

output "deployer_service_account" {
  description = "Service account the GitHub Action impersonates to deploy the app layer."
  value       = google_service_account.deployer.email
}

output "runtime_service_account_email" {
  description = "Service account attached to the app VM (reads secrets at runtime)."
  value       = google_service_account.vm.email
}

output "secret_ids" {
  description = "Secret Manager IDs to seed once with `make cloud-seed-secrets`."
  value       = [for s in google_secret_manager_secret.this : s.secret_id]
}

output "artifact_registry_repo" {
  description = "Docker repo path images are pushed to / GKE pulls from."
  value       = "${google_artifact_registry_repository.images.location}-docker.pkg.dev/${var.project_id}/${google_artifact_registry_repository.images.repository_id}"
}

output "workload_identity_ksa" {
  description = "Kubernetes SA (namespace/name) bound to the runtime GSA for keyless API access."
  value       = "${local.workload_identity_namespace}/${local.workload_identity_ksa}"
}
