#!/usr/bin/env bash
#
# Change the wholesale order rate at runtime.
#
# The rate is ABSOLUTE and independent of scripts/load.sh, so raising consumer
# traffic never multiplies the number of wholesale orders. It starts at 0 — the
# shop runs consumer traffic by default — so this script is how the wholesale
# channel is switched on and off. No helm upgrade and no pod restart is
# involved.
#
# A wholesale order is several distinct products at case quantities, placed
# against the same /api/checkout endpoint a consumer order uses.
#
# Useful range is 0.5-2 rps. Below ~0.2 the share of checkout traffic that is
# wholesale is too small to move storefront-bff's overall error rate past
# Causely's symptom threshold, even though the per-endpoint rate is high.
#
# Usage:
#   scripts/wholesale.sh          # show the current rate
#   scripts/wholesale.sh 0.5      # ~1 wholesale order every 2s
#   scripts/wholesale.sh 0        # off

set -euo pipefail

NAMESPACE="${NAMESPACE:-tracey-shop}"
RELEASE="${RELEASE:-tracey-shop}"
ADMIN_PORT="${ADMIN_PORT:-8090}"
LOCAL_PORT="${LOCAL_PORT:-18093}"

log()  { printf '\033[36m==>\033[0m %s\n' "$*"; }
warn() { printf '\033[33mwarn\033[0m %s\n' "$*" >&2; }
die()  { printf '\033[31merror\033[0m %s\n' "$*" >&2; exit 1; }

command -v kubectl >/dev/null || die "kubectl is required"
command -v curl >/dev/null || die "curl is required"

RPS="${1:-}"

LOADGEN_NAME="${LOADGEN_NAME:-web-client}"
DEPLOY="${RELEASE}-${LOADGEN_NAME}"
kubectl -n "$NAMESPACE" get "deployment/$DEPLOY" >/dev/null 2>&1 \
  || die "no deployment $DEPLOY in namespace $NAMESPACE (is loadgen.enabled=true? override the name with LOADGEN_NAME=)"

kubectl -n "$NAMESPACE" port-forward "deployment/$DEPLOY" "${LOCAL_PORT}:${ADMIN_PORT}" \
  >/dev/null 2>&1 &
PF_PID=$!
trap 'kill $PF_PID 2>/dev/null || true' EXIT

for _ in $(seq 1 40); do
  curl -fsS --max-time 1 "http://127.0.0.1:${LOCAL_PORT}/healthz" >/dev/null 2>&1 && break
  sleep 0.25
done

if [[ -z "$RPS" ]]; then
  log "current load configuration (wholesaleRps is the wholesale order rate)"
  curl -fsS "http://127.0.0.1:${LOCAL_PORT}/admin/load"
  echo
  exit 0
fi

[[ "$RPS" =~ ^[0-9]+([.][0-9]+)?$ ]] || die "rps must be a non-negative number, got '$RPS'"

log "setting wholesale order rate to ${RPS} rps"
curl -fsS -X POST -H 'Content-Type: application/json' \
  -d "{\"wholesaleRps\":${RPS}}" "http://127.0.0.1:${LOCAL_PORT}/admin/load"
echo

# Warn rather than refuse: a trickle is a legitimate way to confirm the channel
# works, it just will not move the storefront's aggregate error rate.
if awk "BEGIN{exit !($RPS > 0 && $RPS < 0.2)}"; then
  warn "${RPS} rps is a trickle; the checkout endpoint will show it but"
  warn "storefront-bff's overall error rate may stay under Causely's threshold"
fi
