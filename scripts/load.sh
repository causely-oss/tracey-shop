#!/usr/bin/env bash
#
# Change the load generator's request rate at runtime.
#
# Useful mid-demo: raising load makes a symptom cross Causely's detection
# thresholds sooner, and no helm upgrade or pod restart is involved.
#
# Usage:
#   scripts/load.sh          # show the current rate
#   scripts/load.sh 80       # drive 80 rps
#   scripts/load.sh 0        # pause traffic
#   scripts/load.sh 40 24    # 40 rps, and record concurrency 24 (applies on restart)

set -euo pipefail

NAMESPACE="${NAMESPACE:-tracey-shop}"
RELEASE="${RELEASE:-tracey-shop}"
ADMIN_PORT="${ADMIN_PORT:-8090}"
LOCAL_PORT="${LOCAL_PORT:-18091}"

log() { printf '\033[36m==>\033[0m %s\n' "$*"; }
die() { printf '\033[31merror\033[0m %s\n' "$*" >&2; exit 1; }

command -v kubectl >/dev/null || die "kubectl is required"
command -v curl >/dev/null || die "curl is required"

RPS="${1:-}"
CONCURRENCY="${2:-}"

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
  log "current load configuration"
  curl -fsS "http://127.0.0.1:${LOCAL_PORT}/admin/load"
  echo
  exit 0
fi

[[ "$RPS" =~ ^[0-9]+([.][0-9]+)?$ ]] || die "rps must be a non-negative number, got '$RPS'"

BODY="{\"rps\":${RPS}"
if [[ -n "$CONCURRENCY" ]]; then
  [[ "$CONCURRENCY" =~ ^[0-9]+$ ]] || die "concurrency must be an integer"
  BODY="${BODY},\"concurrency\":${CONCURRENCY}"
fi
BODY="${BODY}}"

log "setting load to ${RPS} rps"
curl -fsS -X POST -H 'Content-Type: application/json' \
  -d "$BODY" "http://127.0.0.1:${LOCAL_PORT}/admin/load"
echo

if [[ -n "$CONCURRENCY" ]]; then
  log "note: concurrency takes effect on the next loadgen restart"
fi
