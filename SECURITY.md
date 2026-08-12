# Security

## Scope

This repository is a **portfolio reference implementation** of a streaming telemetry pipeline. It
runs two ways: as a local Docker Compose stack, and as a cloud-native deployment on GKE Autopilot in
a GCP project provisioned entirely by Terraform.

**No personal data is processed.** The source is a simulator emitting synthetic temperature, humidity
and pressure readings for five fictional sensors, with a deliberate ~20% anomaly rate. There is no
PII anywhere in the pipeline and no real hardware has ever fed it.

Read this alongside [What this does not do](README.md#what-this-does-not-do) in the README. Nothing
is claimed here that the code does not do.

## Reporting a vulnerability

Open a [GitHub issue](https://github.com/theofanis-tsakanikas/realtime-telemetry-pipeline/issues) for
anything non-sensitive. For something that should not be public, email the address on the
[GitHub profile](https://github.com/theofanis-tsakanikas) with `SECURITY` in the subject. There is no
bug bounty and no SLA; this is a personal project and a best-effort response is what it can honestly
promise.

Only `main` is supported. There are no maintained release branches.

---

## What is hardened

### No service-account key exists anywhere

This is the property the cloud design is built around, and it holds at all three boundaries:

| Boundary | Mechanism |
|---|---|
| GitHub Actions → GCP | **Workload Identity Federation** — a short-lived OIDC token, with an attribute condition pinning the provider to this repository |
| Pod → Google APIs (BigQuery, Secret Manager) | **Workload Identity** — the Kubernetes service account is bound to a runtime GSA; no key is mounted |
| `kubectl` → the cluster | **Connect Gateway** — no bastion, no public control-plane endpoint, no kubeconfig with embedded credentials |

There is no `credentials.json` to leak, rotate or forget, and none is stored as a repository secret.

### Secrets never enter a manifest

Values live in **Secret Manager**, seeded once by `make bootstrap`. At runtime the GKE-managed
**Secrets Store CSI** provider pulls them and materialises a Kubernetes Secret when the first pod
mounts the `SecretProviderClass` — so the manifests in [`infra/k8s/base/`](infra/k8s/base/) reference
secrets by name and never contain a value. Nothing sensitive is committed, and nothing sensitive
lives in the Terraform state for the app layer.

### The cluster is private

GKE Autopilot with **private nodes and a private control-plane endpoint**, a
`master_authorized_networks` configuration, and **Cloud NAT** for egress. There is no public ingress
to the pipeline: Grafana, Redis and the Spark UI are reachable only from inside the cluster or
through a port-forward over Connect Gateway.

### CI

- `permissions:` is explicit and minimal on every workflow — `contents: read`, with `id-token: write`
  only where federation actually happens.
- The `terraform apply` job targets the **`production` GitHub Environment**, and deploys are
  `workflow_dispatch`-only: a merge to `main` cannot provision infrastructure.
- [`gitleaks`](.github/workflows/gitleaks.yml) scans the **full git history** (`fetch-depth: 0`) on
  every push and pull request, not just the diff.
- Dependabot **security** updates remain enabled; routine version updates are switched off
  deliberately — see [`.github/dependabot.yml`](.github/dependabot.yml).

### Data lifecycle

Both BigQuery landing tables are day-partitioned with a **30-day expiry**, so the demo cannot
accumulate data indefinitely, and Terraform state lives in a GCS bucket rather than on anyone's
laptop.

---

## Known limitations

Deliberate trade-offs for a demonstrator. Each is listed with what a deployment would do instead.

### 1. The WIF attribute condition is scoped to the repository, not to a branch or environment

`infra/terraform/foundation/wif.tf` pins the provider with
`attribute_condition = "assertion.repository == '<owner>/<repo>'"`. That correctly stops **other**
repositories from federating — but any workflow in **this** repository, on any branch, satisfies it.
A pull request that adds a workflow can obtain a token with the deployer service account's
permissions.

*A deployment would* add `assertion.ref` and `assertion.environment` to the condition, so only the
default branch and the `production` environment can federate, and would bind the deployer service
account to the narrower identity. **This is the single most valuable hardening left on the list.**

### 2. Kafka has no authentication or transport encryption

The broker runs in KRaft mode with `replication-factor=1`, no SASL, no TLS. Any workload that can
reach the broker's port can produce to `sensor_data` or read the stream — including poisoning the
drift baselines by producing plausible-but-wrong readings.

*A deployment would* enable SASL/SCRAM or mTLS, run ≥3 brokers with `min.insync.replicas=2`, and
restrict producer identities by ACL. Inside the private cluster the exposure is limited to workloads
already on the network, which is why this is acceptable *here* and nowhere else.

### 3. Default credentials ship in the configuration

The local stack defaults to a Redis password of `iot-streaming-demo` and Grafana `admin`/`admin`
unless `.env` overrides them, and the Compose services publish on all interfaces. On an untrusted
network the local stack is reachable from the LAN with known credentials.

*Before running the local stack anywhere shared:* change both in `.env`, and prefix the published
ports in `infra/docker-compose.yml` with `127.0.0.1:`.

### 4. Secret values pass through the developer's machine

`make bootstrap` seeds Secret Manager from a local `.env`. The values therefore exist in plaintext on
that machine, and their lifetime is whatever that file's is.

*A deployment would* create the secrets directly (`gcloud secrets create` from a password manager, or
a provisioning pipeline) and never materialise them on a workstation.

### 5. `deletion_protection = false` on the GKE cluster

Deliberate — the app layer is designed to be destroyed after a demo, and deletion protection would
make `make cloud-down` fail. It also means a mistaken `terraform destroy` takes the cluster with no
confirmation beyond Terraform's own.

### 6. The Streamlit app has no authentication

[`app/`](app/) is a demo surface, deployable standalone. Anyone who can reach the port can read
whatever the configured backend exposes.

### 7. No SBOM and no container vulnerability scanning

`gitleaks` covers secrets and Dependabot covers advisories in declared dependencies, but nothing
scans the three built images or publishes an SBOM. The base images are pinned, which bounds the
problem without solving it.

*A deployment would* add a Trivy or Grype scan to the image build and publish an SBOM alongside each
tag.

### 8. The `production` environment has no required reviewer

Deploys target it, which gives scoping — but the approval gate that would make it a control requires
GitHub Pro or above on a private repository and is not configured.

---

## Pre-publish checklist

Before making the repository public, and after any change to the infrastructure layer:

- [ ] `gitleaks` is green over the **full history**, not just the latest diff
- [ ] `git ls-files` lists no `.env`, `*.tfstate`, `credentials.json` or key material
- [ ] No real GCP project id, project number or WIF provider path is legible in a committed screenshot
- [ ] The WIF attribute condition has been re-read — see limitation 1
- [ ] `.env.example` still contains placeholders, not values
- [ ] Secret Manager holds the real values and no manifest contains one
