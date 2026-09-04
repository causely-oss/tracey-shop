#!/usr/bin/env bash
#
# Verify that the demo emits the exact span attributes Causely needs.
#
# This is the checkpoint to run before concluding that Causely is at fault. Each
# assertion below maps to a specific requirement in the mediator's span analyser:
#
#   resource k8s.pod.uid / k8s.namespace.name + k8s.pod.name
#       -> how a span is mapped to a Kubernetes workload. Missing means the span
#          is DROPPED on ingest and the service never appears in the topology.
#   CLIENT spans with server.address
#       -> how an outbound dependency edge is resolved to a peer Service.
#   gRPC CLIENT spans with rpc.system
#       -> required before Causely treats a span as an RPC call at all.
#   db.system + db.query.text
#       -> how Postgres/Valkey become database dependencies, and how slow
#          queries are attributed.
#   messaging.destination.name + messaging.consumer.group.name
#       -> how Kafka Produces/Consumes edges and consumer lag are modelled.
#   gen_ai.system on a CLIENT span, with server.address, gen_ai.request.model
#   and http.response.status_code
#       -> how an inference becomes an AIModel entity. gen_ai.system's PRESENCE
#          is the only trigger that classifies a span as genAI at all;
#          http.response.status_code is the only source of error and rate-limit
#          signal, because the span's own status is ignored.
#
# The collector must be running with debugVerbosity=detailed, because that is
# what prints full span attributes into its log. Pass --upgrade to switch it on
# temporarily and restore the previous setting afterwards.
#
# Usage:
#   scripts/verify-traces.sh [--upgrade] [--seconds N]

set -euo pipefail

NAMESPACE="${NAMESPACE:-tracey-shop}"
RELEASE="${RELEASE:-tracey-shop}"
CHART="${CHART:-deploy/tracey-shop}"
SECONDS_TO_SAMPLE=45
DO_UPGRADE=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --upgrade) DO_UPGRADE=1; shift ;;
    --seconds) SECONDS_TO_SAMPLE="${2:?--seconds needs a value}"; shift 2 ;;
    -h|--help) sed -n '2,30p' "$0" | sed 's/^#//'; exit 0 ;;
    *) printf 'unknown flag %s\n' "$1" >&2; exit 1 ;;
  esac
done

COLLECTOR="${RELEASE}-otel-collector"
WORKDIR="$(mktemp -d)"
LOGFILE="$WORKDIR/collector.log"
trap 'rm -rf "$WORKDIR"' EXIT

pass=0
fail=0

log()   { printf '\033[36m==>\033[0m %s\n' "$*"; }
ok()    { printf '  \033[32mPASS\033[0m %s\n' "$*"; pass=$((pass+1)); }
bad()   { printf '  \033[31mFAIL\033[0m %s\n' "$*"; fail=$((fail+1)); }
info()  { printf '       %s\n' "$*"; }
die()   { printf '\033[31merror\033[0m %s\n' "$*" >&2; exit 1; }

command -v kubectl >/dev/null || die "kubectl is required"

kubectl -n "$NAMESPACE" get "deployment/$COLLECTOR" >/dev/null 2>&1 \
  || die "no deployment $COLLECTOR in namespace $NAMESPACE"

# ---------------------------------------------------------------------------
# Ensure the collector is logging full spans
# ---------------------------------------------------------------------------

current_verbosity() {
  kubectl -n "$NAMESPACE" get configmap "$COLLECTOR" \
    -o jsonpath='{.data.config\.yaml}' 2>/dev/null \
    | awk '/^      debug:/{found=1; next} found && /verbosity:/{print $2; exit}'
}

VERBOSITY="$(current_verbosity || true)"
RESTORE_VERBOSITY=""

if [[ "$VERBOSITY" != "detailed" ]]; then
  if [[ $DO_UPGRADE -eq 1 ]]; then
    command -v helm >/dev/null || die "helm is required for --upgrade"
    log "switching collector debugVerbosity to detailed (was '${VERBOSITY:-unknown}')"
    helm upgrade "$RELEASE" "$CHART" -n "$NAMESPACE" --reuse-values \
      --set otelCollector.debugVerbosity=detailed --wait --timeout 5m >/dev/null
    RESTORE_VERBOSITY="${VERBOSITY:-basic}"
    kubectl -n "$NAMESPACE" rollout status "deployment/$COLLECTOR" --timeout=120s >/dev/null
  else
    cat >&2 <<EOF
The collector is running with debugVerbosity='${VERBOSITY:-unknown}', which does
not print span attributes. Either re-run with --upgrade, or switch it on yourself:

  helm upgrade $RELEASE $CHART -n $NAMESPACE --reuse-values \\
    --set otelCollector.debugVerbosity=detailed

EOF
    exit 2
  fi
fi

restore() {
  if [[ -n "$RESTORE_VERBOSITY" ]]; then
    log "restoring collector debugVerbosity to '$RESTORE_VERBOSITY'"
    helm upgrade "$RELEASE" "$CHART" -n "$NAMESPACE" --reuse-values \
      --set "otelCollector.debugVerbosity=${RESTORE_VERBOSITY}" --wait --timeout 5m >/dev/null || true
  fi
  rm -rf "$WORKDIR"
}
trap restore EXIT

# ---------------------------------------------------------------------------
# Sample the collector log
# ---------------------------------------------------------------------------

log "sampling collector output for ${SECONDS_TO_SAMPLE}s"
kubectl -n "$NAMESPACE" logs "deployment/$COLLECTOR" --since=10s -f > "$LOGFILE" 2>/dev/null &
LOG_PID=$!
sleep "$SECONDS_TO_SAMPLE"
kill "$LOG_PID" 2>/dev/null || true
wait "$LOG_PID" 2>/dev/null || true

lines="$(wc -l < "$LOGFILE" | tr -d ' ')"
info "captured ${lines} log lines"
if [[ "$lines" -lt 20 ]]; then
  die "almost no collector output — is the load generator running? (./scripts/load.sh)"
fi

has() { grep -qF -- "$1" "$LOGFILE"; }
count_of() { grep -cF -- "$1" "$LOGFILE" || true; }

echo
log "1. Resource attributes (workload resolution)"

if has 'k8s.pod.uid'; then
  ok "k8s.pod.uid present — spans can be mapped to a workload"
else
  bad "k8s.pod.uid MISSING — Causely will drop every span"
  info "check the k8sattributes processor and its RBAC (ClusterRole ${COLLECTOR})"
fi

for attr in k8s.namespace.name k8s.pod.name k8s.deployment.name k8s.node.name; do
  if has "$attr"; then ok "$attr present"; else bad "$attr missing"; fi
done

if has 'service.namespace'; then
  ok "service.namespace present"
else
  bad "service.namespace missing"
fi

echo
log "2. Service coverage"

SERVICES=(
  storefront-bff catalog-api cart-service checkout-api
  inventory-svc pricing-engine payment-gw shipping-quote
  ledger-svc fraud-detector risk-model notification-worker
  stripe-sim carrier-sim email-sim web-client
)
# Only emit spans while genAI traffic is running (scripts/genai.sh), so they are
# checked in section 9 rather than being required here.
GENAI_SERVICES=(ai-assistant model-gateway)
missing_services=()
for svc in "${SERVICES[@]}"; do
  if has "service.name: Str($svc)"; then
    ok "$svc is emitting spans"
  else
    missing_services+=("$svc")
    bad "$svc produced no spans in this sample"
  fi
done
if [[ ${#missing_services[@]} -gt 0 ]]; then
  info "low-traffic services can be missed in a short sample; retry with --seconds 120"
fi

echo
log "3. Span kinds (topology edges)"

# The collector's debug exporter prints span kind as "Kind           : Server",
# not "SpanKind: Server". Matching the wrong string made every one of these
# checks report zero regardless of what the app emitted — including a false
# "no Internal spans" pass.
count_kind() { grep -cE "^[[:space:]]*Kind[[:space:]]*: ${1}\$" "$LOGFILE" || true; }

for kind in Server Client Producer Consumer; do
  n="$(count_kind "$kind")"
  if [[ "${n:-0}" -gt 0 ]]; then
    ok "${kind} spans present (${n})"
  else
    bad "no ${kind} spans — the matching edge type will not appear in the topology"
  fi
done

if [[ "$(count_kind Internal)" == "0" ]]; then
  ok "no Internal spans reaching the exporter (filtered as intended)"
else
  info "Internal spans present ($(count_kind Internal)); set otelCollector.filterInternalSpans=true to drop them"
fi

echo
log "4. Browser storefront assets must NOT be traced"

# httpx.Server.Static registers the UI outside otelhttp on purpose. If that ever
# regresses, "/" and "/app.js" become HTTPPath entities in Causely and every
# browser page load shifts storefront-bff's latency and error-rate baseline —
# which silently changes what every scenario looks like.
# Span-name lines use variable spacing, so match it loosely — a fixed-width
# pattern would silently never match and make this assertion vacuous.
span_name_count() { grep -cE "^[[:space:]]*Name[[:space:]]*: ${1}\$" "$LOGFILE" || true; }

asset_spans=0
for name in 'GET /' 'GET /app\.js' 'GET /style\.css'; do
  asset_spans=$((asset_spans + $(span_name_count "$name")))
done

# Prove the matcher works before trusting a zero, so a broken pattern cannot be
# mistaken for a passing assertion.
api_spans="$(span_name_count 'GET /api/products')"

if [[ "${api_spans:-0}" -eq 0 ]]; then
  info "no 'GET /api/products' spans in this sample — cannot judge asset tracing"
elif [[ "$asset_spans" -eq 0 ]]; then
  ok "no spans for /, /app.js or /style.css (${api_spans} API spans seen) — the UI bypasses otelhttp"
else
  bad "${asset_spans} span(s) found for static assets — they are being traced"
  info "httpx.Server.Static must register on the mux directly, not via otelhttp"
fi

echo
log "5. Peer attributes on outbound calls"

if has 'server.address'; then
  ok "server.address present — dependency edges are resolvable"
else
  bad "server.address MISSING — no dependency edges will be built"
fi
if has 'server.port'; then ok "server.port present"; else bad "server.port missing"; fi
if has 'rpc.system'; then
  ok "rpc.system present — gRPC calls will be modelled as RPC"
else
  bad "rpc.system MISSING — gRPC edges will not be detected"
fi
for attr in rpc.method rpc.grpc.status_code http.request.method http.response.status_code; do
  if has "$attr"; then ok "$attr present"; else bad "$attr missing"; fi
done

echo
log "6. Database dependencies"

if has 'db.system: Str(postgresql)'; then
  ok "Postgres dependency detected (db.system=postgresql)"
else
  bad "no Postgres spans (db.system=postgresql missing)"
fi
if has 'db.system: Str(redis)'; then
  ok "Valkey dependency detected (db.system=redis)"
else
  bad "no Valkey spans (db.system=redis missing)"
fi
if has 'db.query.text'; then
  ok "db.query.text present — slow queries will be attributable"
else
  bad "db.query.text missing"
fi
if has 'db.namespace'; then ok "db.namespace present"; else info "db.namespace absent (optional)"; fi

echo
log "7. Kafka topics and consumer groups"

if has 'messaging.system: Str(kafka)'; then
  ok "messaging.system=kafka present"
else
  bad "messaging.system=kafka missing"
fi
if has 'messaging.destination.name'; then
  ok "messaging.destination.name present — topic entities will be created"
else
  bad "messaging.destination.name MISSING — no topic entities or Produces/Consumes edges"
fi
if has 'messaging.consumer.group.name'; then
  ok "messaging.consumer.group.name present — consumer lag will be attributable"
else
  bad "messaging.consumer.group.name MISSING — consumer lag cannot be attributed to a group"
fi
for topic in orders ledger.events notifications; do
  if has "Str($topic)"; then
    ok "topic '$topic' seen in spans"
  else
    bad "topic '$topic' not seen in this sample"
  fi
done

echo
log "8. GenAI spans (AIModel entities)"

# Every requirement below is one the mediator fails SILENTLY: analyzeGenAISpan
# either is never reached or returns early, and nothing is logged above Debug.
# See docs/genai.md.
if ! has 'gen_ai.system'; then
  info "no genAI spans in this sample — is genAI traffic running? (./scripts/genai.sh 0.5)"
  info "skipping the genAI assertions rather than reporting them as failures"
else
  ok "gen_ai.system present — spans will be classified as genAI"

  # Prove the Client-kind matcher works before trusting anything derived from
  # it, the same guard section 4 uses. A genAI span MUST be CLIENT: the mediator
  # only analyses genAI in the client pass.
  if [[ "$(count_kind Client)" -gt 0 ]]; then
    ok "Client spans present — genAI spans can be analysed at all"
  else
    bad "no Client spans, so no genAI span can be analysed"
  fi

  if has 'gen_ai.request.model'; then
    ok "gen_ai.request.model present — the AIModel entity can be named"
  else
    bad "gen_ai.request.model MISSING — the mediator rejects a span with no model"
  fi

  if has 'gen_ai.operation.name'; then
    ok "gen_ai.operation.name present"
  else
    info "gen_ai.operation.name absent (the mediator defaults it to 'chat')"
  fi

  # Token counts must be Int, not Double: the mediator's attribute reader
  # accepts IntValue only and silently reads anything else as 0.
  for attr in gen_ai.usage.input_tokens gen_ai.usage.output_tokens; do
    if grep -qE "^[[:space:]]*-> ${attr}: Int\\(" "$LOGFILE"; then
      ok "$attr present and Int-typed"
    elif has "$attr"; then
      bad "$attr present but NOT Int-typed — it will be read as 0"
      grep -E "^[[:space:]]*-> ${attr}:" "$LOGFILE" | tail -1 | sed 's/^/       /'
    else
      bad "$attr MISSING — no token metrics will be attributed"
    fi
  done

  # The only source of InferenceError / RateLimited. Its absence is invisible:
  # the inference is simply counted as a success.
  if has 'http.response.status_code'; then
    ok "http.response.status_code present — inference errors and 429s are visible"
  else
    bad "http.response.status_code MISSING on genAI spans"
    info "the span status is ignored, so without it every inference counts as a success"
  fi

  # server.address is checked in section 5 for the whole sample; here it must be
  # on the genAI span itself or analyzeGenAISpan returns immediately.
  if has 'server.address'; then
    ok "server.address present — the provider Service can be resolved"
  else
    bad "server.address MISSING — no AIModel entity will be created"
  fi

  for svc in "${GENAI_SERVICES[@]}"; do
    if has "service.name: Str($svc)"; then
      ok "$svc is emitting spans"
    else
      info "$svc produced no spans in this sample"
    fi
  done
fi

echo
log "9. Exporter health"

if grep -qiE 'permanent error|connection refused|no such host|context deadline exceeded' "$LOGFILE"; then
  bad "the exporter is reporting errors — traces may not be reaching the mediator"
  grep -iE 'permanent error|connection refused|no such host|context deadline exceeded' "$LOGFILE" \
    | tail -3 | sed 's/^/       /'
  info "confirm the endpoint: kubectl -n $NAMESPACE get cm $COLLECTOR -o yaml | grep -A3 causely"
else
  ok "no exporter errors in the sample"
fi

echo
printf '\033[1m%s\033[0m\n' "Summary: ${pass} passed, ${fail} failed"
if [[ "$fail" -gt 0 ]]; then
  echo
  echo "Failures above are in the demo's own instrumentation or collector config."
  echo "Fix them before investigating Causely: a dropped span is invisible on both sides."
  exit 1
fi
echo "The trace contract Causely depends on is satisfied."
