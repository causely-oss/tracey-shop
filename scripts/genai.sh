#!/usr/bin/env bash
#
# Change the genAI request rate at runtime.
#
# The rate is ABSOLUTE and independent of scripts/load.sh, so raising shop
# traffic never multiplies spend against a real LLM provider. It starts at 0 —
# the genAI services deploy and stay idle — so this script is how the genAI path
# is switched on and off. No helm upgrade and no pod restart is involved.
#
# Useful range is 0.2-0.5 rps. Below ~0.1 Causely's Inference Latency symptom can
# never fire: it is gated on InferenceTotalRate > 0.1, so a slower trickle still
# builds the AIModel entity and its token metrics but can never produce a latency
# signal.
#
# Usage:
#   scripts/genai.sh          # show the current rate
#   scripts/genai.sh 0.5      # ~1 inference every 2s
#   scripts/genai.sh 0        # off

set -euo pipefail

NAMESPACE="${NAMESPACE:-tracey-shop}"
RELEASE="${RELEASE:-tracey-shop}"
ADMIN_PORT="${ADMIN_PORT:-8090}"
LOCAL_PORT="${LOCAL_PORT:-18092}"

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

# The rate is applied to the load generator, but the traffic is only useful if
# the assistant is actually deployed. Checking here turns a silent 404 storm into
# one clear message.
if ! kubectl -n "$NAMESPACE" get "deployment/${RELEASE}-ai-assistant" >/dev/null 2>&1; then
  die "no deployment ${RELEASE}-ai-assistant in namespace $NAMESPACE — set genai.enabled=true and helm upgrade first"
fi

kubectl -n "$NAMESPACE" port-forward "deployment/$DEPLOY" "${LOCAL_PORT}:${ADMIN_PORT}" \
  >/dev/null 2>&1 &
PF_PID=$!
trap 'kill $PF_PID 2>/dev/null || true' EXIT

for _ in $(seq 1 40); do
  curl -fsS --max-time 1 "http://127.0.0.1:${LOCAL_PORT}/healthz" >/dev/null 2>&1 && break
  sleep 0.25
done

if [[ -z "$RPS" ]]; then
  log "current load configuration (assistRps is the genAI rate)"
  curl -fsS "http://127.0.0.1:${LOCAL_PORT}/admin/load"
  echo
  exit 0
fi

[[ "$RPS" =~ ^[0-9]+([.][0-9]+)?$ ]] || die "rps must be a non-negative number, got '$RPS'"

log "setting genAI rate to ${RPS} rps"
curl -fsS -X POST -H 'Content-Type: application/json' \
  -d "{\"assistRps\":${RPS}}" "http://127.0.0.1:${LOCAL_PORT}/admin/load"
echo

# Warn rather than refuse: a deliberately tiny rate is a legitimate way to prove
# ingestion works without spending anything, it just cannot produce a latency
# symptom later.
if awk "BEGIN{exit !($RPS > 0 && $RPS <= 0.1)}"; then
  warn "${RPS} rps is at or below Causely's InferenceTotalRate > 0.1 activation gate;"
  warn "entities and token metrics will appear but Inference Latency can never fire"
fi
