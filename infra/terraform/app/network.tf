# --------------------------------------------------------------------------- #
# Network: a custom VPC hosting the GKE Autopilot cluster. Nodes are private (no
# external IPs) and reach the internet (image pulls, Maven) through Cloud NAT.
# Dashboards are reached via `kubectl port-forward`, not a public LoadBalancer.
# --------------------------------------------------------------------------- #

resource "google_compute_network" "vpc" {
  name                    = "telemetry-vpc"
  auto_create_subnetworks = false
}

resource "google_compute_subnetwork" "subnet" {
  name          = "telemetry-subnet"
  ip_cidr_range = "10.10.0.0/24"
  region        = var.region
  network       = google_compute_network.vpc.id

  private_ip_google_access = true

  # VPC flow logs — network observability + security baseline (CKV_GCP_26).
  log_config {
    aggregation_interval = "INTERVAL_5_SEC"
    flow_sampling        = 0.5
    metadata             = "INCLUDE_ALL_METADATA"
  }

  # Secondary ranges for the VPC-native GKE Autopilot cluster (pods + services).
  secondary_ip_range {
    range_name    = "pods"
    ip_cidr_range = "10.20.0.0/16"
  }

  secondary_ip_range {
    range_name    = "services"
    ip_cidr_range = "10.30.0.0/20"
  }
}

# Cloud NAT: outbound internet for the private GKE nodes (no external IPs).
resource "google_compute_router" "router" {
  name    = "telemetry-router"
  region  = var.region
  network = google_compute_network.vpc.id
}

resource "google_compute_router_nat" "nat" {
  name                               = "telemetry-nat"
  router                             = google_compute_router.router.name
  region                             = var.region
  nat_ip_allocate_option             = "AUTO_ONLY"
  source_subnetwork_ip_ranges_to_nat = "ALL_SUBNETWORKS_ALL_IP_RANGES"
}
