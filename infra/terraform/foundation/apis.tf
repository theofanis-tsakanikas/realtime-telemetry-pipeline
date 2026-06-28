# Every API the whole stack needs, enabled once in the foundation layer.
locals {
  required_apis = [
    "cloudresourcemanager.googleapis.com", # read/manage the project + IAM
    "iam.googleapis.com",                  # service accounts + IAM
    "iamcredentials.googleapis.com",       # short-lived creds for WIF impersonation
    "sts.googleapis.com",                  # security token service (WIF token exchange)
    "serviceusage.googleapis.com",         # enabling APIs
    "compute.googleapis.com",              # the app VM, VPC, firewall, NAT
    "secretmanager.googleapis.com",        # Redis / Slack / Grafana / deploy-key secrets
    "iap.googleapis.com",                  # IAP TCP tunnels to the dashboards
    "oslogin.googleapis.com",              # IAM-based SSH over IAP
    "bigquery.googleapis.com",             # analytics sink (Spark → BigQuery) + dbt
    "bigquerystorage.googleapis.com",      # BigQuery Storage Write API (streaming inserts)
    "container.googleapis.com",            # GKE Autopilot cluster (the stack runtime)
    "artifactregistry.googleapis.com",     # Docker registry for the stack images
  ]
}

resource "google_project_service" "required" {
  for_each = toset(local.required_apis)

  project            = var.project_id
  service            = each.value
  disable_on_destroy = false
}
