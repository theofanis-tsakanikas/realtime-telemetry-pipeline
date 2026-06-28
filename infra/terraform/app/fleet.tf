# --------------------------------------------------------------------------- #
# Fleet membership → Connect Gateway. Registering the cluster to the project
# fleet lets the deploy workflow run `kubectl` against it **keylessly via the
# gateway** (gcloud container fleet memberships get-credentials), with no direct
# line-of-sight to the control-plane endpoint. So the control plane can be locked
# down (var.control_plane_authorized_cidrs) without breaking CI deploys.
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
