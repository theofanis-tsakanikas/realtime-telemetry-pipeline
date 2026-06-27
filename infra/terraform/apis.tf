# Every API the stack needs, enabled as code.
locals {
  required_apis = [
    "cloudresourcemanager.googleapis.com", # read/manage the project + IAM
    "iam.googleapis.com",                  # service accounts + IAM
    "iamcredentials.googleapis.com",       # short-lived creds for WIF impersonation
    "sts.googleapis.com",                  # security token service (WIF token exchange)
    "compute.googleapis.com",              # the VM, VPC, firewall, NAT
    "secretmanager.googleapis.com",        # Redis / Slack / Grafana secrets
    "iap.googleapis.com",                  # IAP TCP tunnels to the dashboards
    "oslogin.googleapis.com",              # IAM-based SSH over IAP
  ]
}

resource "google_project_service" "required" {
  for_each = toset(local.required_apis)

  project = var.project_id
  service = each.value

  # Keep APIs enabled if this config is ever destroyed — other things may use them.
  disable_on_destroy = false
}
