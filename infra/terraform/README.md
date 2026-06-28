# Terraform (GCP) — two layers

The infrastructure is split into two independently-stated layers, so the CI deployer can
manage the app with **least privilege** and **never touches its own auth foundation**.

```
infra/terraform/
├── foundation/   # Layer 0 — run ONCE by the owner (seed). Persists.
│                 #   WIF + deployer SA + runtime SA + Secret Manager containers + scoped IAM
└── app/          # Layer 1 — routine spin-up / tear-down (CLI or CI).
                  #   VPC + firewall (IAP-only) + Cloud NAT + the stack VM
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
make cloud-plan      # preview
make cloud-up        # deploy app: network + VM → boots the stack   (no secret re-push!)
make cloud-tunnels   # IAP tunnels → http://localhost:3300 (Grafana), :8085, :9090, ...
make cloud-pause     # stop the VM overnight (data kept)
make cloud-resume
make cloud-down      # destroy the app layer → ~$0 (foundation + secrets stay)
```

Because the **secrets are a seed** (Layer 0), an app deploy is genuinely **one step** —
`make cloud-up` references the already-seeded secrets; nothing to re-push.

## Why two layers (the professional reasons)

- **Least privilege:** the `gha-deployer` SA only gets `compute.admin` + `serviceAccountUser`
  + state-bucket object access — exactly what the app layer needs. No IAM/WIF/secret admin.
- **No self-reference:** the deployer never manages the WIF/SA that authenticate it, so CI can
  apply/destroy the app layer cleanly (and via a GitHub Actions button) without destroying its
  own identity.
- **Secrets out of the deploy lifecycle:** seeded once, never re-pushed, never in git/tfstate.

## Production note

The app layer is a single VM running Docker Compose — a deliberate, cost-efficient choice for an
ephemeral demo. In production you'd decompose it into managed services (Confluent Cloud,
Dataproc/GKE, Redis Enterprise) and build images in CI → Artifact Registry. See the main README.
