# Terraform (GCP) — two layers

The infrastructure is split into two independently-stated layers, so the CI deployer can
manage the app with **least privilege** and **never touches its own auth foundation**.

```
infra/terraform/
├── foundation/   # Layer 0 — run ONCE by the owner (seed). Persists.
│                 #   WIF + deployer SA + runtime SA + Secret Manager + Artifact
│                 #   Registry + BigQuery (analytics history) + scoped IAM
└── app/          # Layer 1 — routine spin-up / tear-down (CLI or CI).
                  #   VPC + Cloud NAT + the GKE Autopilot cluster (the stack runtime)
```

State lives in the manually-seeded GCS bucket `realtime-telemetry-gcp-tfstate` (prefixes
`foundation` and `app`). The bucket is kept **outside** Terraform on purpose.

## One-time setup (owner)

```bash
# 0. Seed bucket (once)
gcloud storage buckets create gs://realtime-telemetry-gcp-tfstate \
  --project=realtime-telemetry-gcp --location=europe-west3 \
  --uniform-bucket-level-access --public-access-prevention
gcloud storage buckets update gs://realtime-telemetry-gcp-tfstate --versioning

# 1. Bootstrap: foundation apply (WIF + SAs + Secret Manager + Artifact Registry +
#    BigQuery) AND seed the secret VALUES from your local .env, in one command.
make bootstrap
```

The foundation can't be a workflow — it bootstraps the very WIF that authenticates CI — so it's
owner/CLI, run once. The secret values are seeded from `.env` here and then persist; nothing
sensitive ever touches git or tfstate.

## Routine lifecycle (the deploy button)

Spin-up / tear-down is **one GitHub Action** (`Terraform (GCP)` → *Run workflow*), keyless via WIF
and gated by the `production` environment:

| `action` | What it does |
|---|---|
| `plan` | `terraform plan` the app layer |
| `apply` | `terraform apply` (VPC + GKE) **then** `kubectl apply` the stack via Connect Gateway |
| `destroy` | best-effort `kubectl delete` (releases PVs) **then** `terraform destroy` → ~$0 |

Images are built separately by the **Build images** workflow (on push to the image files, or
on-demand), tagged `:<sha>` + `:latest` — deploy only pulls them, never builds.

Equivalent **CLI** path (same Makefile targets the workflow calls):

```bash
make cloud-up && make k8s-images && make k8s-apply   # bring up
make cloud-down                                       # tear down (foundation + BigQuery stay)
```

Reach the dashboards with `kubectl port-forward` (see [`infra/k8s/README.md`](../k8s/README.md)) —
no public LoadBalancer, same no-public-ingress posture the VM had. The cluster reads secrets from
Secret Manager via the CSI driver; an app deploy never re-pushes them.

## Why two layers (the professional reasons)

- **Least privilege:** the `gha-deployer` SA only gets `compute.admin` + `serviceAccountUser`
  + `container.admin` + `artifactregistry.writer` + state-bucket object access — exactly what the
  app layer needs. No IAM/WIF/secret/BigQuery admin.
- **No self-reference:** the deployer never manages the WIF/SA that authenticate it, so CI can
  apply/destroy the app layer cleanly (and via a GitHub Actions button) without destroying its
  own identity.
- **Secrets + data out of the deploy lifecycle:** secrets seeded once (never re-pushed, never in
  git/tfstate); BigQuery lives in the foundation so analytics history survives app teardown.

## Production note

The stack runs on GKE Autopilot as a single-node-per-service demo — a deliberate, cost-efficient
choice for an ephemeral deployment. In production you'd lean on managed services (Confluent Cloud,
Dataproc, Redis Enterprise) and scale the StatefulSets out (Kafka RF=3, etc.). See the main README.
