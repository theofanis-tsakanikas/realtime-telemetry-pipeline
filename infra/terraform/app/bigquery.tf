# --------------------------------------------------------------------------- #
# Analytics sink: the cleaned readings and the rejected rows are streamed to
# BigQuery (in addition to Redis). Redis = real-time serving; BigQuery = the
# analytics layer (SQL, dbt marts, Looker Studio). Part of the app layer, so it
# is created/destroyed with each ephemeral deploy.
# --------------------------------------------------------------------------- #

resource "google_bigquery_dataset" "telemetry" {
  dataset_id                 = var.bigquery_dataset
  location                   = var.bigquery_location
  description                = "Real-time telemetry analytics (streamed from Spark; modelled with dbt)."
  delete_contents_on_destroy = true # ephemeral demo — teardown removes the data
}

resource "google_bigquery_table" "readings" {
  dataset_id          = google_bigquery_dataset.telemetry.dataset_id
  table_id            = "readings"
  deletion_protection = false
  description         = "Cleaned, in-range sensor readings (valid branch)."

  time_partitioning {
    type  = "DAY"
    field = "event_time"
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

  schema = jsonencode([
    { name = "sensor_id", type = "STRING", mode = "NULLABLE" },
    { name = "rejection_reason", type = "STRING", mode = "REQUIRED" },
    { name = "event_time", type = "TIMESTAMP", mode = "NULLABLE" },
    { name = "ingest_time", type = "TIMESTAMP", mode = "REQUIRED" },
  ])
}
