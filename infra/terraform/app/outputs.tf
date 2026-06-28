output "cluster_name" {
  description = "GKE Autopilot cluster name."
  value       = google_container_cluster.autopilot.name
}

output "cluster_credentials_hint" {
  description = "Fetch kubectl credentials for the cluster."
  value       = "gcloud container clusters get-credentials ${google_container_cluster.autopilot.name} --region ${var.region} --project ${var.project_id}"
}
