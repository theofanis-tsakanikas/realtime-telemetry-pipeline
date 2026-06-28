# --------------------------------------------------------------------------- #
# GKE Workload Identity wiring. The actual binding (KSA → runtime GSA) lives in
# the APP layer, NOT here: the PROJECT.svc.id.goog identity pool only springs
# into existence once a cluster with Workload Identity is created, so it cannot
# be bound in the foundation (which runs before any cluster exists).
#
# What the foundation does instead: grant the deployer the ability to set the
# runtime SA's IAM policy — scoped to that ONE SA — so the app layer can create
# the workloadIdentityUser binding without project-wide service-account admin.
# --------------------------------------------------------------------------- #

locals {
  workload_identity_namespace = "telemetry"
  workload_identity_ksa       = "telemetry-runtime"
}

resource "google_service_account_iam_member" "deployer_runtime_admin" {
  service_account_id = google_service_account.vm.name
  role               = "roles/iam.serviceAccountAdmin"
  member             = "serviceAccount:${google_service_account.deployer.email}"
}
