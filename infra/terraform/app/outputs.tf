output "cluster_name" {
  description = "GKE Autopilot cluster name."
  value       = google_container_cluster.autopilot.name
}

output "cluster_credentials_hint" {
  description = "Fetch kubectl credentials directly (needs control-plane endpoint access)."
  value       = "gcloud container clusters get-credentials ${google_container_cluster.autopilot.name} --region ${var.region} --project ${var.project_id}"
}

output "gateway_credentials_hint" {
  description = "Fetch kubectl credentials via Connect Gateway (keyless; works with a locked-down endpoint — what CI uses)."
  value       = "gcloud container fleet memberships get-credentials ${google_gke_hub_membership.autopilot.membership_id} --project ${var.project_id}"
}
