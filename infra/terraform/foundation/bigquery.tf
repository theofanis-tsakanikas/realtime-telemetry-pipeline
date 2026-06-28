# --------------------------------------------------------------------------- #
# BigQuery analytics layer. Lives in the FOUNDATION (not the ephemeral app
# layer) on purpose: data has a different lifecycle than compute. `terraform
# destroy` of the app tears down the cluster but LEAVES the analytics history
# here — it persists across spin-ups. Storage at this volume is ~$0 (free tier),
# and a 30-day partition expiration keeps it self-pruning so it never grows
# unbounded. The runtime SA (Spark via Workload Identity) writes here through the
# Storage Write API; dbt models the marts in the same dataset.
# --------------------------------------------------------------------------- #

locals {
  # 30 days, in milliseconds — partition expiration for the streamed tables.
  partition_expiration_ms = 30 * 24 * 60 * 60 * 1000
}

resource "google_bigquery_dataset" "telemetry" {
  dataset_id                 = var.bigquery_dataset
  location                   = var.bigquery_location
  description                = "Real-time telemetry analytics (streamed from Spark; modelled with dbt)."
  delete_contents_on_destroy = true # only ever destroyed when decommissioning the foundation
}

resource "google_bigquery_table" "readings" {
  dataset_id          = google_bigquery_dataset.telemetry.dataset_id
  table_id            = "readings"
  deletion_protection = false
  description         = "Cleaned, in-range sensor readings (valid branch)."

  time_partitioning {
    type          = "DAY"
    field         = "event_time"
    expiration_ms = local.partition_expiration_ms
  }

  schema = jsonencode([
    { name = "sensor_id", type = "STRING", mode = "REQUIRED" },
    { name = "temperature", type = "FLOAT", mode = "NULLABLE" },
    { name = "humidity", type = "FLOAT", mode = "NULLABLE" },
    { name = "pressure", type = "FLOAT", mode = "NULLABLE" },
    { name = "event_time", type = "TIMESTAMP", mode = "REQUIRED" },
    { name = "ingest_time", type = "TIMESTAMP", mode = "REQUIRED" },
  ])
}

resource "google_bigquery_table" "rejections" {
  dataset_id          = google_bigquery_dataset.telemetry.dataset_id
  table_id            = "rejections"
  deletion_protection = false
  description         = "Rejected rows with the contract rule they violated (DLQ analytics)."

  # event_time is nullable (a row can be rejected for a missing timestamp), so
  # partition on the always-present ingest_time.
  time_partitioning {
    type          = "DAY"
    field         = "ingest_time"
    expiration_ms = local.partition_expiration_ms
  }

  schema = jsonencode([
    { name = "sensor_id", type = "STRING", mode = "NULLABLE" },
    { name = "rejection_reason", type = "STRING", mode = "REQUIRED" },
    { name = "event_time", type = "TIMESTAMP", mode = "NULLABLE" },
    { name = "ingest_time", type = "TIMESTAMP", mode = "REQUIRED" },
  ])
}

# The runtime SA (Spark + the dbt CronJob, on GKE via Workload Identity) writes
# rows and runs jobs. Granted here so the identity persists across app deploys.
locals {
  runtime_bigquery_roles = [
    "roles/bigquery.dataEditor", # write rows / let dbt build the marts
    "roles/bigquery.jobUser",    # run load / Storage Write API / query jobs
  ]
}

resource "google_project_iam_member" "vm_bigquery" {
  for_each = toset(local.runtime_bigquery_roles)

  project = var.project_id
  role    = each.value
  member  = "serviceAccount:${google_service_account.vm.email}"
}
