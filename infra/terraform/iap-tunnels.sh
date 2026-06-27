#!/usr/bin/env bash
# Open IAP TCP tunnels to the stack VM's dashboards. Ctrl+C closes them all.
# Usage: ./iap-tunnels.sh [vm_name] [zone]
set -euo pipefail

VM="${1:-telemetry-stack}"
ZONE="${2:-europe-west3-a}"

# name:port pairs
SERVICES=(
  "grafana:3000"
  "kafka-ui:8085"
  "prometheus:9090"
  "redisinsight:8001"
  "spark-ui:4040"
  "schema-registry:8081"
)

pids=()
cleanup() { kill "${pids[@]}" 2>/dev/null || true; }
trap cleanup EXIT INT TERM

echo "Opening IAP tunnels to $VM ($ZONE)…"
for svc in "${SERVICES[@]}"; do
  name="${svc%%:*}"
  port="${svc##*:}"
  gcloud compute start-iap-tunnel "$VM" "$port" \
    --local-host-port="localhost:$port" --zone="$ZONE" >/dev/null 2>&1 &
  pids+=("$!")
  printf '  %-15s http://localhost:%s\n' "$name" "$port"
done

echo "Tunnels up. Press Ctrl+C to close."
wait
