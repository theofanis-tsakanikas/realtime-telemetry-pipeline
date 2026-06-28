# --------------------------------------------------------------------------- #
# Docker registry for the stack images (simulator, spark). Lives in the
# foundation layer so the images persist across ephemeral app (GKE) deploys —
# you push once, then spin the cluster up/down without rebuilding.
# --------------------------------------------------------------------------- #

data "google_project" "this" {
  project_id = var.project_id
}

resource "google_artifact_registry_repository" "images" {
  project       = var.project_id
  location      = var.region
  repository_id = "telemetry"
  format        = "DOCKER"
  description   = "Container images for the telemetry stack (pulled by GKE Autopilot)."

  depends_on = [google_project_service.required]
}

# GKE pulls images with the NODE identity (the default compute SA on Autopilot),
# not the workload SA — so the reader grant goes there. Scoped to this repo only.
resource "google_artifact_registry_repository_iam_member" "node_pull" {
  project    = var.project_id
  location   = google_artifact_registry_repository.images.location
  repository = google_artifact_registry_repository.images.repository_id
  role       = "roles/artifactregistry.reader"
  member     = "serviceAccount:${data.google_project.this.number}-compute@developer.gserviceaccount.com"
}
