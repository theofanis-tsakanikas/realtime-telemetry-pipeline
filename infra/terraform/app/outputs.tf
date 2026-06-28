output "cluster_name" {
  description = "GKE Autopilot cluster name."
  value       = google_container_cluster.autopilot.name
}

output "gateway_credentials_hint" {
  description = "Fetch kubectl credentials via Connect Gateway (keyless, IP-independent; the only way in — the control plane is private). Used by you and CI alike."
  value       = "gcloud container fleet memberships get-credentials ${google_gke_hub_membership.autopilot.membership_id} --project ${var.project_id}"
}
