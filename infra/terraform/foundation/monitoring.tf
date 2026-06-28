# GKE Managed Service for Prometheus (GMP): managed collection scrapes the Spark
# metrics endpoint, and the in-cluster query frontend reads them back to serve
# Grafana. The frontend runs as the runtime SA (Workload Identity) and needs to
# read Cloud Monitoring time series. Granted here so it persists across deploys.
resource "google_project_iam_member" "vm_monitoring_viewer" {
  project = var.project_id
  role    = "roles/monitoring.viewer"
  member  = "serviceAccount:${google_service_account.vm.email}"
}
