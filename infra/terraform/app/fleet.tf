# --------------------------------------------------------------------------- #
# Fleet membership → Connect Gateway. Registering the cluster to the project
# fleet lets you AND the deploy workflow run `kubectl` against it **keylessly via
# the gateway** (gcloud container fleet memberships get-credentials), authenticated
# by IAM identity regardless of source IP. This is the only way in — the control
# plane is private (no public endpoint), so there is no IP allowlist to maintain.
#
# Lives in the app layer: the membership's lifecycle matches the cluster's — it
# is created and destroyed together with each ephemeral deploy.
# --------------------------------------------------------------------------- #

resource "google_gke_hub_membership" "autopilot" {
  membership_id = var.cluster_name

  endpoint {
    gke_cluster {
      resource_link = "//container.googleapis.com/${google_container_cluster.autopilot.id}"
    }
  }

  depends_on = [google_container_cluster.autopilot]
}
