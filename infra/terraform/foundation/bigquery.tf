# The runtime SA (Spark, on the VM / later GKE via Workload Identity) writes the
# cleaned readings + rejections to BigQuery via the Storage Write API. Granted here
# in the foundation so the identity persists across app deploys.

locals {
  runtime_bigquery_roles = [
    "roles/bigquery.dataEditor", # write rows into the dataset's tables
    "roles/bigquery.jobUser",    # run load / Storage Write API sessions
  ]
}

resource "google_project_iam_member" "vm_bigquery" {
  for_each = toset(local.runtime_bigquery_roles)

  project = var.project_id
  role    = each.value
  member  = "serviceAccount:${google_service_account.vm.email}"
}
