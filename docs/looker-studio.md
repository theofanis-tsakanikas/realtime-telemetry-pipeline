# Looker Studio — BI on the BigQuery marts

Looker Studio is the executive/analytics view of the pipeline, sitting on the
**dbt marts** in BigQuery (`telemetry` dataset). It complements Grafana: Grafana
is the live operational view (Redis time series + GMP system metrics); Looker
Studio is the historical, shareable, business-facing report. Looker Studio is a
hosted BI tool, so it isn't provisioned as code — this is the connect-and-build
guide.

## Prerequisites

1. The app stack is deployed and the Spark job has written data to BigQuery.
2. The dbt marts are built: `make dbt-build` (see [`dbt/README.md`](../dbt/README.md)).
   The report reads the marts, not the raw tables.

## Connect

1. Open <https://lookerstudio.google.com> → **Create → Data source → BigQuery**.
2. Project `realtime-telemetry-gcp` → dataset `telemetry` → pick a mart table.
   Add one data source per mart you want to chart:

   | Mart | Grain | Good for |
   |---|---|---|
   | `sensor_minutely` | (sensor_id, minute) | per-sensor temperature/humidity/pressure trends |
   | `reading_volume` | minute | fleet ingestion throughput + active sensor count |
   | `accept_rate_minutely` | minute | data-quality KPI (accept rate, accepted vs rejected) |
   | `rejections_by_reason` | (minute, reason) | what's failing — rejects broken down by rule |

3. For each, set `minute` as the **date/time dimension** (type: Date & Time → minute).

## Suggested report pages

- **Overview** — scorecards: latest `accept_rate` (from `accept_rate_minutely`),
  current `reading_count`/`active_sensors` (from `reading_volume`); a time series
  of `accept_rate` over the last 24h.
- **Sensors** — time series of `avg_temperature` / `avg_humidity` / `avg_pressure`
  from `sensor_minutely`, broken down by `sensor_id`.
- **Data quality** — stacked bar of `rejection_count` by `rejection_reason` over
  time (`rejections_by_reason`); table of the top reasons.

## Notes

- The marts are **rolling windows** (6–24h). Schedule `dbt build` (cron / Cloud
  Scheduler) so the report stays fresh; Looker Studio caches can be set to refresh
  every 15 min.
- Access uses the viewer's Google identity against BigQuery — no service-account
  keys. Grant report viewers `roles/bigquery.dataViewer` on the dataset if you
  share beyond yourself.
- Keep heavy aggregation in dbt (already minute-bucketed) so Looker Studio scans
  small, partition-pruned marts rather than the raw `readings` table.
