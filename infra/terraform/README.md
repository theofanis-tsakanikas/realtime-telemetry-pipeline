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

# 1. Foundation: identity + secret containers + runtime SA
make cloud-foundation-up

# 2. Seed secret VALUES once (from your .env). They persist across every app deploy.
make cloud-seed-secrets
```

## Routine lifecycle (app layer — CLI or CI)

```bash
make cloud-plan      # preview the app-layer changes
make cloud-up        # deploy: VPC + Cloud NAT + GKE Autopilot cluster
make k8s-images      # build + push the stack images → Artifact Registry
make k8s-apply       # deploy the stack manifests onto the cluster
make cloud-down      # destroy the app layer → ~$0 (foundation + BigQuery + secrets stay)
```

Reach the dashboards with `kubectl port-forward` (see [`infra/k8s/README.md`](../k8s/README.md)) —
no public LoadBalancer, same no-public-ingress posture the VM had. Because the **secrets are a
seed** (Layer 0), an app deploy never re-pushes them; the cluster reads them from Secret Manager
via the CSI driver.

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
