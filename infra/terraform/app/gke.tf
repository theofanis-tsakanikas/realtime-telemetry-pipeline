# --------------------------------------------------------------------------- #
# GKE Autopilot — the runtime for the containerised stack (Kafka, Spark, Redis,
# Grafana, simulator, ...). Autopilot manages the nodes; we pay per pod request
# and scale to the workloads in Phase D's manifests.
#
# VPC-native, in the same VPC as the rest of the app layer. Private nodes (no
# public IPs; egress via the existing Cloud NAT). The control plane keeps a
# public endpoint locked to var.control_plane_authorized_cidrs so kubectl works
# from your machine without a bastion — narrow it to your IP before applying.
# Workload Identity (always on in Autopilot) lets pods authenticate as the
# runtime GSA; the binding lives in the foundation layer.
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

  private_cluster_config {
    enable_private_nodes    = true
    enable_private_endpoint = false
    master_ipv4_cidr_block  = "172.16.0.0/28"
  }

  master_authorized_networks_config {
    dynamic "cidr_blocks" {
      for_each = var.control_plane_authorized_cidrs
      content {
        cidr_block   = cidr_blocks.value.cidr_block
        display_name = cidr_blocks.value.display_name
      }
    }
  }

  # Ephemeral demo cluster — allow `terraform destroy` to tear it down.
  deletion_protection = false
}
