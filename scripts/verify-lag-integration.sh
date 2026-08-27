#!/usr/bin/env bash
#
# Verify the Kafka consumer-lag pipeline end to end.
#
# This is the one scenario that cannot work from traces alone. The chain is:
#
#   Kafka group offsets
#     -> kafka-exporter          publishes kafka_consumergroup_lag{topic,consumergroup}
#     -> Prometheus              scrapes it via the ServiceMonitor
#     -> Causely Prometheus scraper  max(kafka_consumergroup_lag) by (namespace, topic, consumergroup)
#     -> Lag attribute on the ConsumerTopicAccess entity
#     -> "Consumer Lag" symptom  when the 5-minute average exceeds 50
#
# Every step is checked separately, because a break in any one of them looks
# identical from Causely: no symptom at all.
#
# Usage:
#   scripts/verify-lag-integration.sh

set -euo pipefail

NAMESPACE="${NAMESPACE:-tracey-shop}"
RELEASE="${RELEASE:-tracey-shop}"
PROM_NAMESPACE="${PROM_NAMESPACE:-monitoring}"
PROM_SERVICE="${PROM_SERVICE:-prometheus-operated}"
PROM_PORT="${PROM_PORT:-9090}"

pass=0
fail=0

log()  { printf '\033[36m==>\033[0m %s\n' "$*"; }
ok()   { printf '  \033[32mPASS\033[0m %s\n' "$*"; pass=$((pass+1)); }
bad()  { printf '  \033[31mFAIL\033[0m %s\n' "$*"; fail=$((fail+1)); }
info() { printf '       %s\n' "$*"; }
die()  { printf '\033[31merror\033[0m %s\n' "$*" >&2; exit 1; }

command -v kubectl >/dev/null || die "kubectl is required"
command -v curl >/dev/null || die "curl is required"
command -v python3 >/dev/null || die "python3 is required"

# pf <namespace> <svc-or-pod> <local:remote> — port-forward, wait, echo the pid
pf_start() {
  kubectl -n "$1" port-forward "$2" "$3" >/dev/null 2>&1 &
  echo $!
}

# ---------------------------------------------------------------------------
# 1. The exporter is running and publishing lag
# ---------------------------------------------------------------------------

log "1. kafka-exporter"

if ! kubectl -n "$NAMESPACE" get "deployment/${RELEASE}-kafka-exporter" >/dev/null 2>&1; then
  bad "no deployment ${RELEASE}-kafka-exporter — is causelyIntegrations.kafka.enabled=true?"
  info "without it, consumer lag never reaches Causely and fraud-lag produces no symptom"
  exit 1
fi
ok "deployment ${RELEASE}-kafka-exporter exists"

PF=$(pf_start "$NAMESPACE" "svc/${RELEASE}-kafka-exporter" "19308:9308")
trap 'kill $PF 2>/dev/null || true' EXIT
for _ in $(seq 1 40); do
  curl -fsS --max-time 1 localhost:19308/metrics >/dev/null 2>&1 && break
  sleep 0.25
done

METRICS="$(curl -fsS --max-time 10 localhost:19308/metrics 2>/dev/null || true)"
kill "$PF" 2>/dev/null || true
trap - EXIT

if [[ -z "$METRICS" ]]; then
  bad "could not scrape the exporter's /metrics"
  exit 1
fi

LAG_LINES="$(grep '^kafka_consumergroup_lag{' <<<"$METRICS" || true)"
if [[ -n "$LAG_LINES" ]]; then
  ok "exporter publishes kafka_consumergroup_lag ($(wc -l <<<"$LAG_LINES" | tr -d ' ') series)"
  while IFS= read -r l; do info "$l"; done <<<"$(head -6 <<<"$LAG_LINES")"
else
  bad "exporter publishes no kafka_consumergroup_lag series"
  info "kafka_exporter only emits it for groups that have COMMITTED offsets;"
  info "if no consumer has ever committed, there is nothing to report yet"
fi

# The label names are what Causely matches on — wrong names mean silent failure.
if grep -q 'kafka_consumergroup_lag{.*consumergroup=' <<<"$METRICS" \
   && grep -q 'kafka_consumergroup_lag{.*topic=' <<<"$METRICS"; then
  ok "labels are 'topic' and 'consumergroup', which is what Causely's query groups by"
else
  bad "lag series lack the topic/consumergroup labels Causely requires"
fi

# ---------------------------------------------------------------------------
# 2. Prometheus is scraping it
# ---------------------------------------------------------------------------

log "2. Prometheus"

if ! kubectl -n "$PROM_NAMESPACE" get "svc/$PROM_SERVICE" >/dev/null 2>&1; then
  info "no ${PROM_NAMESPACE}/${PROM_SERVICE}; skipping the Prometheus checks"
  info "set PROM_NAMESPACE / PROM_SERVICE if yours differs"
else
  if kubectl -n "$NAMESPACE" get servicemonitor "${RELEASE}-kafka-exporter" >/dev/null 2>&1; then
    ok "ServiceMonitor ${RELEASE}-kafka-exporter exists"
  else
    bad "no ServiceMonitor — Prometheus will not discover the exporter"
    info "serviceMonitor.enabled=false, or the prometheus-operator CRD is absent"
  fi

  PF=$(pf_start "$PROM_NAMESPACE" "svc/$PROM_SERVICE" "19090:${PROM_PORT}")
  trap 'kill $PF 2>/dev/null || true' EXIT
  for _ in $(seq 1 40); do
    curl -fsS --max-time 1 localhost:19090/-/ready >/dev/null 2>&1 && break
    sleep 0.25
  done

  # Exactly the query Causely's shipped kafka exporter config runs.
  RESULT="$(curl -sfG --data-urlencode \
    'query=max(kafka_consumergroup_lag) by (namespace, topic, consumergroup)' \
    localhost:19090/api/v1/query 2>/dev/null || true)"
  kill "$PF" 2>/dev/null || true
  trap - EXIT

  SERIES="$(python3 -c "
import json,sys
try: d=json.loads(sys.argv[1])
except Exception: print('ERR'); raise SystemExit
r=d.get('data',{}).get('result',[])
ours=[x for x in r if x['metric'].get('namespace')=='${NAMESPACE}']
print(len(ours))
for x in ours:
    m=x['metric']
    print('  %s / %s = %s' % (m.get('consumergroup'), m.get('topic'), x['value'][1]))
" "$RESULT" 2>/dev/null || echo ERR)"

  COUNT="$(head -1 <<<"$SERIES")"
  if [[ "$COUNT" == "ERR" ]]; then
    bad "could not query Prometheus"
  elif [[ "${COUNT:-0}" -gt 0 ]]; then
    ok "Causely's exact query returns ${COUNT} series for ${NAMESPACE}"
    tail -n +2 <<<"$SERIES" | while IFS= read -r l; do info "$l"; done
  else
    bad "Causely's query returns no series for namespace ${NAMESPACE}"
    info "Prometheus may not have scraped yet (30s interval) — retry shortly"
  fi
fi

# ---------------------------------------------------------------------------
# 3. The entity the metric must attach to
# ---------------------------------------------------------------------------

log "3. ConsumerTopicAccess entities (created by traces, not by the metric)"

cat <<'EOF'
       The Lag metric can only attach to a ConsumerTopicAccess entity that
       already exists, matched on its ConsumerGroup and ConsumesTopic labels.
       Those come from the messaging.consumer.group.name span attribute set in
       internal/transport/kafkax — the metric alone will never create the entity.

       Confirm with the Causely MCP:
         get_entities(namespace_names=["tracey-shop"],
                      entity_types=["ConsumerTopicAccess"])
       Expect labels ConsumerGroup=fraud-workers, ConsumesTopic=orders.

       Then, ~6 minutes after starting the fraud-lag scenario:
         get_symptoms(namespace_names=["tracey-shop"],
                      entity_types=["ConsumerTopicAccess"])
       Expect the "Consumer Lag" symptom. It fires when the 5-minute average
       exceeds 50 messages, and Causely's Prometheus scraper syncs every 60s.
EOF

echo
printf '\033[1m%s\033[0m\n' "Summary: ${pass} passed, ${fail} failed"
[[ "$fail" -gt 0 ]] && exit 1
echo "The Kubernetes-side lag pipeline is wired correctly."
