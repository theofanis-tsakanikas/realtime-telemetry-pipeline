# --------------------------------------------------------------------------- #
# GKE Autopilot — the runtime for the containerised stack (Kafka, Spark, Redis,
# Grafana, simulator, ...). Autopilot manages the nodes; we pay per pod request
# and scale to the workloads in Phase D's manifests.
#
# VPC-native, in the same VPC as the rest of the app layer. Private nodes (no
# public IPs; egress via the existing Cloud NAT) AND a private control plane (no
# public API endpoint). Reach the cluster via Connect Gateway (you + CI), keyless
# and IP-independent. Workload Identity (always on in Autopilot) lets pods
# authenticate as the runtime GSA; the binding lives in the foundation layer.
# --------------------------------------------------------------------------- #

resource "google_container_cluster" "autopilot" {
  name             = var.cluster_name
  location         = var.region
  enable_autopilot = true

  network    = google_compute_network.vpc.id
  subnetwork = google_compute_subnetwork.subnet.id

  ip_allocation_policy {
    cluster_secondary_range_name  = "pods"
    services_secondary_range_name = "services"
  }

  release_channel {
    channel = "REGULAR"
  }

  # Fully private control plane: no public API endpoint exists at all (so there is
  # nothing to IP-allowlist, and no dynamic-IP problem). Both you and CI reach the
  # cluster via Connect Gateway, which authenticates by IAM identity regardless of
  # source IP. terraform itself is unaffected — it talks to the GKE management API,
  # not the cluster endpoint.
  private_cluster_config {
    enable_private_nodes    = true
    enable_private_endpoint = true
    master_ipv4_cidr_block  = "172.16.0.0/28"
  }

  # GKE requires master authorized networks to be ENABLED whenever the private
  # endpoint is on. We add no CIDR blocks on purpose — access is exclusively via
  # Connect Gateway (which bypasses this), so the endpoint stays maximally locked
  # down. The empty block is what enables the feature (satisfies CKV_GCP_20 too).
  master_authorized_networks_config {
    gcp_public_cidrs_access_enabled = false
  }

  # Managed Secret Manager add-on: installs the Secrets Store CSI provider so the
  # SecretProviderClass can mount the foundation's secrets (Redis / Grafana /
  # Slack) into pods — keyless, via Workload Identity. No K8s Secrets in git.
  secret_manager_config {
    enabled = true
  }

  # Ephemeral demo cluster — allow `terraform destroy` to tear it down.
  deletion_protection = false
}
