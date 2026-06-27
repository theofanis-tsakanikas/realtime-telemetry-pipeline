# --- WIF (for the GitHub Actions workflow, Phase 6) — identifiers, not secrets ---

output "workload_identity_provider" {
  description = "Full provider resource name for google-github-actions/auth (workload_identity_provider)."
  value       = google_iam_workload_identity_pool_provider.github.name
}

output "deployer_service_account" {
  description = "Service account email the GitHub Action impersonates."
  value       = google_service_account.deployer.email
}

# --- Stack VM ---

output "vm_name" {
  description = "Stack VM name (for gcloud stop/start and IAP tunnels)."
  value       = google_compute_instance.stack.name
}

output "vm_zone" {
  description = "Stack VM zone."
  value       = google_compute_instance.stack.zone
}

output "secret_ids" {
  description = "Secret Manager IDs to populate with `make cloud-secrets` before the VM can start the stack."
  value       = [for s in google_secret_manager_secret.this : s.secret_id]
}

output "tunnel_hint" {
  description = "How to open the dashboards."
  value       = "make cloud-tunnels   # then open http://localhost:3000 (Grafana), :8085 (Kafka-UI), :9090 (Prometheus)"
}
