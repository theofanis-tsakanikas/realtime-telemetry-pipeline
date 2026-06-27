# Terraform (GCP) — ephemeral full-stack deployment

Provisions the **entire pipeline** on GCP (Frankfurt) on a single VM, with **remote state**,
**keyless GitHub Actions deploys** (Workload Identity Federation), and **no public ingress**
(IAP-only). Designed to be spun up for a demo/recording and torn down after — pay only for the
hours it runs (~$0.13/hr, e2-standard-4).

This is the portfolio's **second cloud** (the first project provisions AWS) — same keyless/OIDC
philosophy, different provider.

## One-time seed (done once, by hand)

The state bucket is kept **outside** Terraform on purpose (Terraform must never be able to
destroy the store holding its own state):

```bash
gcloud storage buckets create gs://realtime-telemetry-gcp-tfstate \
  --project=realtime-telemetry-gcp --location=europe-west3 \
  --uniform-bucket-level-access --public-access-prevention
gcloud storage buckets update gs://realtime-telemetry-gcp-tfstate --versioning
```

## What it creates

| Group | Resources |
|---|---|
| **Foundation** | API enablement, Workload Identity Federation (pool/provider/deployer SA) |
| **Network** | custom VPC + subnet, IAP-only firewall (no public ingress), Cloud NAT (outbound) |
| **Secrets** | Secret Manager containers (Redis / Slack / Grafana) — values pushed out-of-band |
| **Compute** | e2-standard-4 VM (no external IP) + runtime SA; startup script runs the full stack |

## Daily workflow

```bash
make cloud-up        # terraform apply: infra + VM boots and starts the stack
make cloud-secrets   # push .env secrets into Secret Manager (VM waits ~10 min for them)
make cloud-tunnels   # open IAP tunnels → http://localhost:3000 (Grafana), :8085, :9090, ...

make cloud-pause     # end of day: stop the VM (no compute charge, data kept)
make cloud-resume    # next day: start it; the stack auto-resumes

make cloud-down      # fully done: destroy everything → $0
```

## Cost & safety

- **~$0.13/hr** running (~$1 per working day); **~$0.05/night** when paused (disk only); **$0** after destroy.
- The VM **auto-stops after `auto_stop_minutes`** (default 8h) as a safety net if you forget to pause.
- No external IP, IAP-only access; secrets live in Secret Manager (never in git or tfstate).

## Notes

- In production you'd decompose this single VM into managed services (Confluent Cloud,
  Dataproc/GKE, Memorystore/Redis Enterprise). The single-VM Docker Compose deployment is a
  deliberate, cost-efficient choice for an ephemeral demo — see the main README.
- Credentials: `gcloud auth application-default login` locally, or WIF in CI.
