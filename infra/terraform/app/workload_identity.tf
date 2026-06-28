# --------------------------------------------------------------------------- #
# Workload Identity binding: let the Kubernetes SA telemetry/telemetry-runtime
# impersonate the runtime GSA (no key files). Created in the APP layer because
# the PROJECT.svc.id.goog identity pool only exists after the cluster enables
# Workload Identity — depends_on the cluster guarantees that ordering. The
# deployer can set IAM on the runtime SA via a scoped grant in the foundation.
# --------------------------------------------------------------------------- #

locals {
  runtime_sa_email = "telemetry-stack-vm@${var.project_id}.iam.gserviceaccount.com"
  wi_namespace     = "telemetry"
  wi_ksa           = "telemetry-runtime"
}

resource "google_service_account_iam_member" "gke_workload_identity" {
  service_account_id = "projects/${var.project_id}/serviceAccounts/${local.runtime_sa_email}"
  role               = "roles/iam.workloadIdentityUser"
  member             = "serviceAccount:${var.project_id}.svc.id.goog[${local.wi_namespace}/${local.wi_ksa}]"

  depends_on = [google_container_cluster.autopilot]
}
