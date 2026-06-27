# --------------------------------------------------------------------------- #
# Network: a custom VPC with NO public ingress. The VM has no external IP; you
# reach its dashboards through IAP TCP tunnels, and it reaches the internet
# (Docker Hub, GitHub, Maven for Spark JARs) through Cloud NAT.
# --------------------------------------------------------------------------- #

resource "google_compute_network" "vpc" {
  name                    = "telemetry-vpc"
  auto_create_subnetworks = false
  depends_on              = [google_project_service.required]
}

resource "google_compute_subnetwork" "subnet" {
  name          = "telemetry-subnet"
  ip_cidr_range = "10.10.0.0/24"
  region        = var.region
  network       = google_compute_network.vpc.id

  # Lets the no-external-IP VM reach Google APIs (Secret Manager) privately.
  private_ip_google_access = true
}

# Allow ONLY Google's IAP range to reach SSH + the dashboard ports. No 0.0.0.0/0.
resource "google_compute_firewall" "iap_ingress" {
  name      = "allow-iap-ingress"
  network   = google_compute_network.vpc.name
  direction = "INGRESS"

  # Google IAP forwarding range — traffic only ever arrives via authenticated IAP.
  source_ranges = ["35.235.240.0/20"]
  target_tags   = ["telemetry-stack"]

  allow {
    protocol = "tcp"
    # 22=SSH, 3000=Grafana, 4040=Spark UI, 5540=RedisInsight, 8001=RedisInsight(embedded),
    # 8081=Schema Registry, 8085=Kafka-UI, 9090=Prometheus
    ports = ["22", "3000", "4040", "5540", "8001", "8081", "8085", "9090"]
  }
}

# Cloud NAT: outbound internet for the no-external-IP VM (image pulls, git, JARs).
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
