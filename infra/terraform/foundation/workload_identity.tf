# --------------------------------------------------------------------------- #
# GKE Workload Identity: let the stack's Kubernetes service account impersonate
# the runtime GSA (which already holds Secret Manager + BigQuery access), so pods
# authenticate to Google APIs with NO key files — the same keyless model the VM
# used, now for GKE. The binding is on the GSA, so it lives in the foundation
# (the least-privilege app deployer can't modify the runtime SA's IAM).
#
# The member principal is deterministic from project + namespace + KSA name, so
# it can be created before the cluster exists. The app layer's K8s manifests
# create namespace `telemetry` + KSA `telemetry-runtime` and annotate it back.
# --------------------------------------------------------------------------- #

locals {
  workload_identity_namespace = "telemetry"
  workload_identity_ksa       = "telemetry-runtime"
}

resource "google_service_account_iam_member" "gke_workload_identity" {
  service_account_id = google_service_account.vm.name
  role               = "roles/iam.workloadIdentityUser"
  member             = "serviceAccount:${var.project_id}.svc.id.goog[${local.workload_identity_namespace}/${local.workload_identity_ksa}]"
}
