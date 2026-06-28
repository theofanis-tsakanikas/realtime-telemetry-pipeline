#!/usr/bin/env bash
# Push secret VALUES from the local .env into Secret Manager (never via Terraform,
# so secrets stay out of tfstate). Run once as part of `make bootstrap`, after the
# foundation creates the (empty) secret containers. The values then persist across
# every app deploy; the GKE stack reads them via the Secret Manager CSI driver.
#
# Uses only indexed arrays (no `declare -A`) so it works on macOS's bash 3.2.
set -euo pipefail

PROJECT="${GCP_PROJECT_ID:-realtime-telemetry-gcp}"
ENV_FILE="${1:-.env}"

if [[ ! -f "$ENV_FILE" ]]; then
  echo "ERROR: $ENV_FILE not found." >&2
  exit 1
fi

# Read a KEY from the .env without sourcing it (avoids executing arbitrary content).
read_env() {
  grep -E "^$1=" "$ENV_FILE" 2>/dev/null | tail -1 | cut -d= -f2- || true
}

# secret-id : .env-key
PAIRS=(
  "telemetry-redis-password:REDIS_PASSWORD"
  "telemetry-slack-webhook-url:SLACK_WEBHOOK_URL"
  "telemetry-grafana-admin-password:GRAFANA_ADMIN_PASSWORD"
)

for pair in "${PAIRS[@]}"; do
  secret="${pair%%:*}"
  key="${pair##*:}"
  value="$(read_env "$key")"
  if [[ -z "$value" ]]; then
    echo "WARN: $key empty in $ENV_FILE — skipping $secret" >&2
    continue
  fi
  printf '%s' "$value" | gcloud secrets versions add "$secret" \
    --project="$PROJECT" --data-file=- >/dev/null
  echo "  pushed $secret  (from $key)"
done

# Deploy key is a file (read-only SSH key), not an .env value.
DEPLOY_KEY_FILE="infra/terraform/.secrets/deploy_key"
if [[ -f "$DEPLOY_KEY_FILE" ]]; then
  gcloud secrets versions add telemetry-deploy-key \
    --project="$PROJECT" --data-file="$DEPLOY_KEY_FILE" >/dev/null
  echo "  pushed telemetry-deploy-key  (from $DEPLOY_KEY_FILE)"
else
  echo "WARN: $DEPLOY_KEY_FILE not found — skipping telemetry-deploy-key" >&2
fi

echo "Secrets pushed. The VM will pick up the latest versions on boot."
