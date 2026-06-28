# dbt — BigQuery analytics marts

Transforms the raw tables that the Spark job streams into BigQuery (`telemetry.readings`,
`telemetry.rejections`) into a small set of **short-window aggregation marts** for BI
(Looker Studio / Grafana). Redis stays the real-time serving layer; this is the SQL
analytics layer on the same data.

## Layout

```
dbt/
├── dbt_project.yml          # project config (staging → views, marts → tables)
├── profiles.yml             # self-contained BigQuery profile (OAuth/ADC, env-var driven)
├── models/
│   ├── staging/
│   │   ├── _sources.yml      # the two Spark-written tables + source freshness/tests
│   │   ├── stg_readings.sql  # thin view over readings
│   │   └── stg_rejections.sql# thin view over rejections (+ bucket_time = coalesce(event,ingest))
│   └── marts/
│       ├── sensor_minutely.sql       # per-sensor, per-minute rollup (24h)
│       ├── reading_volume.sql        # fleet throughput readings/min + active sensors (6h)
│       ├── rejections_by_reason.sql  # rejects/min by violated rule (24h)
│       ├── accept_rate_minutely.sql  # accepted/rejected/accept_rate per minute (24h)
│       └── _models.yml               # column descriptions + generic tests
└── tests/
    ├── assert_accept_rate_in_range.sql        # accept_rate ∈ [0,1]
    └── assert_sensor_minutely_grain_unique.sql# one row per (sensor_id, minute)
```

## Auth

OAuth / Application Default Credentials — no key files:

- **Local:** `gcloud auth application-default login`
- **On the VM / GKE:** the attached runtime service account
  (`telemetry-stack-vm@…`) already has `bigquery.dataEditor` + `bigquery.jobUser`.

Project / dataset / location are read from env vars with sensible defaults
(`GCP_PROJECT=realtime-telemetry-gcp`, `BIGQUERY_DATASET=telemetry`,
`BIGQUERY_LOCATION=europe-west3`).

## Usage

From the repo root:

```bash
make dbt-setup    # once: create .venv-dbt and install dbt-bigquery
make dbt-parse    # offline validation (no warehouse, no creds) — also runs in CI
make dbt-build    # run + test against BigQuery (needs ADC + data in the tables)
```

Or directly inside this directory:

```bash
DBT_PROFILES_DIR=. dbt build
```

The marts are rolling windows (6–24h), so re-run `dbt build` on a schedule (e.g. cron /
Cloud Scheduler) to keep them fresh. They are materialized as tables for fast BI reads.
