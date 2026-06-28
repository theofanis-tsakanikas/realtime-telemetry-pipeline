output "vm_name" {
  description = "Stack VM name (for gcloud stop/start and IAP tunnels)."
  value       = google_compute_instance.stack.name
}

output "vm_zone" {
  description = "Stack VM zone."
  value       = google_compute_instance.stack.zone
}

output "tunnel_hint" {
  description = "How to open the dashboards."
  value       = "make cloud-tunnels   # then open http://localhost:3300 (Grafana), :8085, :9090, ..."
}
