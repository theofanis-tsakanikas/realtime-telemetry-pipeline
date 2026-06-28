# Kubernetes manifests — the stack on GKE Autopilot

Kustomize base that runs the whole telemetry stack on the GKE Autopilot cluster
from the app-layer Terraform. The same containers as the compose/VM stack, now
as Deployments/StatefulSets, with keyless access to Google APIs via Workload
Identity and secrets pulled from Secret Manager through the CSI driver.

## What's here

```
infra/k8s/base/
├── kustomization.yaml        # resources + ConfigMap generators + image tags
├── namespace.yaml            # namespace: telemetry
├── serviceaccount.yaml       # KSA telemetry-runtime (Workload Identity → runtime GSA)
├── secretproviderclass.yaml  # Secret Manager → synced K8s Secret (telemetry-secrets)
├── kafka/                    # StatefulSet (KRaft single node) + headless Service + PVC
├── schema-registry/          # Deployment + Service
├── redis/                    # StatefulSet (Redis Stack) + Service + PVC
├── simulator/                # Deployment (image from Artifact Registry)
├── spark/                    # Deployment + checkpoint PVC + Service (4040) — BigQuery sink ON
├── kafka-ui/                 # Deployment + Service
├── redis-insight/            # Deployment + Service
├── grafana/                  # Deployment + Service (provisioned from ConfigMaps)
├── gmp/                      # PodMonitoring (scrape Spark) + GMP query frontend
└── dbt/                      # CronJob: `dbt build` every 2 min → refresh the BigQuery marts
```

ConfigMaps for the Grafana provisioning, dashboards and Spark metrics are
**generated from the existing `infra/grafana` and `infra/spark` files** (one
source of truth, shared with the compose/VM stack). Those files live outside the
kustomize root, so rendering uses `--load-restrictor=LoadRestrictionsNone`.

## Secrets (keyless)

No secrets in git. The `SecretProviderClass` pulls `telemetry-redis-password`,
`telemetry-grafana-admin-password` and `telemetry-slack-webhook-url` from Secret
Manager (seeded once in the foundation) and syncs them into a Kubernetes Secret
the pods read via `secretKeyRef`. Auth is Workload Identity — the
`telemetry-runtime` KSA impersonates the runtime GSA, which holds
`secretmanager.secretAccessor` + the BigQuery roles.

## Deploy

```bash
# 1. Point kubectl at the cluster (output of the app-layer Terraform)
gcloud container clusters get-credentials telemetry-autopilot \
  --region europe-west3 --project realtime-telemetry-gcp

# 2. Build + push the simulator and spark images to Artifact Registry
make k8s-images

# 3. Render + apply the stack
make k8s-apply        # = kubectl kustomize --load-restrictor=LoadRestrictionsNone infra/k8s/base | kubectl apply -f -

# Tear down (cluster stays):  make k8s-delete
```

`make k8s-render` prints the rendered manifests without applying — this is what
CI schema-checks (`kubeconform`) on every PR.

## Access the dashboards

No public LoadBalancer / Ingress — same no-public-ingress posture as the VM
(which used IAP tunnels). Reach the UIs over the authenticated control plane with
`kubectl port-forward`:

```bash
kubectl -n telemetry port-forward svc/grafana       3000:3000   # Grafana
kubectl -n telemetry port-forward svc/kafka-ui       8085:8080   # Kafka-UI
kubectl -n telemetry port-forward svc/redis-insight  5540:5540   # RedisInsight
kubectl -n telemetry port-forward svc/spark-processor 4040:4040  # Spark UI
```

## Notes

- **Single-node, demo sizing.** Kafka is one KRaft broker (RF=1); production wants
  3+ brokers, RF=3, min-ISR=2.
- **Spark → BigQuery is ON here** (`BIGQUERY_DATASET=telemetry`), unlike compose.
  Valid rows go to Redis *and* BigQuery; rejected rows to the DLQ topic *and* the
  BigQuery `rejections` table.
- **dbt marts on a schedule.** A `CronJob` (in `dbt/`) runs `dbt build` every 2
  minutes against BigQuery, keyless via the same Workload Identity. The marts are
  short rolling windows, so the tight cadence keeps Looker Studio / the Grafana
  panels current and shows them filling up during a recording. Built from
  `docker/Dockerfile.dbt` (the dbt project baked into an image).
- **System metrics via GMP.** A `PodMonitoring` (in `gmp/`) makes GKE Managed
  Service for Prometheus scrape the Spark driver's `/metrics/prometheus`. An
  in-cluster GMP **query frontend** is exposed as a Service named `prometheus`
  on 9090 — the same name/port the shared Grafana datasource uses — so the
  Pipeline Health dashboard works on GKE with no datasource change. The frontend
  reads Cloud Monitoring via Workload Identity (runtime GSA has `monitoring.viewer`).
