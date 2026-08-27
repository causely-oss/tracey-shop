#!/usr/bin/env bash
#
# Start and stop fault scenarios at runtime.
#
# Faults are applied by POSTing to each pod's admin port, which is reached via
# kubectl port-forward. Nothing is exposed outside the cluster, and no helm
# upgrade or pod restart is involved — so a live demo can move from healthy to
# broken and back in seconds.
#
# Usage:
#   scripts/scenario.sh list
#   scripts/scenario.sh start <scenario>
#   scripts/scenario.sh stop  <scenario>
#   scripts/scenario.sh stop-all
#   scripts/scenario.sh status

set -euo pipefail

NAMESPACE="${NAMESPACE:-tracey-shop}"
RELEASE="${RELEASE:-tracey-shop}"
ADMIN_PORT="${ADMIN_PORT:-8090}"
LOCAL_PORT="${LOCAL_PORT:-18090}"

# Every service that carries a fault store.
ALL_SERVICES=(
  storefront-bff catalog-api cart-service checkout-api
  inventory-svc pricing-engine payment-gw shipping-quote
  ledger-svc fraud-detector risk-model notification-worker
  stripe-sim carrier-sim email-sim web-client
)

# ---------------------------------------------------------------------------
# Scenario definitions
#
# Each scenario is "<service>=<json patch>" pairs. The expected Causely root
# cause is documented in docs/scenarios.md.
# ---------------------------------------------------------------------------

scenario_spec() {
  case "$1" in
    payment-errors)
      # 35% of authorisations fail. Errors surface at storefront-bff three
      # layers up; Causely should name payment-gw, not its callers.
      echo 'payment-gw={"errorRate":0.35}'
      ;;
    payment-outage)
      # Every authorisation fails.
      #
      # Use this when a human is clicking through the browser storefront: at 35%
      # a presenter has a 65% chance of a successful checkout on any given click,
      # and runs of six successes are common. That reads as "the demo is broken"
      # to an audience. 100% makes the failure land on the first click, every
      # time, which is what you want when a person is driving the demo.
      echo 'payment-gw={"errorRate":1.0}'
      ;;
    ledger-slow-queries)
      # 800ms of database time at the bottom of the deepest chain. No errors
      # anywhere — pure latency propagation five hops up.
      echo 'ledger-svc={"slowQueryMs":800}'
      ;;
    inventory-oom)
      # Retains 256KiB per request until the container is OOMKilled, producing
      # restarts alongside the request failures.
      echo 'inventory-svc={"memLeakKbPerReq":256}'
      ;;
    fraud-lag)
      # The consumer stops committing. There is no upstream caller to report an
      # error, so consumer lag on the orders topic is the only signal.
      echo 'fraud-detector={"consumerStall":true}'
      ;;
    pricing-cpu)
      # 40ms of CPU burn per request drives CFS throttling under load.
      echo 'pricing-engine={"cpuBurnMs":40}'
      ;;
    cart-timeouts)
      # A mildly slow dependency plus an aggressive client timeout: cart-service
      # is the true cause, but checkout-api is where the errors appear.
      echo 'cart-service={"latencyMs":1500}' 'checkout-api={"dependencyTimeoutMs":500}'
      ;;
    catalog-cache-miss)
      # Forces every catalogue read through to Postgres, shifting load onto
      # inventory-svc and the database without any error at all.
      echo 'catalog-api={"disableCache":true}'
      ;;
    ledger-pool-exhaustion)
      # Leaks a pool connection per request until the pool starves.
      echo 'ledger-svc={"dbConnLeak":true}'
      ;;
    risk-crash)
      # 2% of requests panic, producing CrashLoopBackOff on the deepest service
      # of the asynchronous branch.
      echo 'risk-model={"panicRate":0.02}'
      ;;
    checkout-latency)
      # Latency injected at the orchestrator itself, as a control case: the
      # cause and the symptom are the same service.
      echo 'checkout-api={"latencyMs":400,"latencyJitterMs":200}'
      ;;
    *)
      return 1
      ;;
  esac
}

SCENARIOS=(
  payment-errors
  payment-outage
  ledger-slow-queries
  inventory-oom
  fraud-lag
  pricing-cpu
  cart-timeouts
  catalog-cache-miss
  ledger-pool-exhaustion
  risk-crash
  checkout-latency
)

scenario_description() {
  case "$1" in
    payment-errors)          echo "payment-gw returns 35% gRPC INTERNAL   -> expect root cause: payment-gw" ;;
    payment-outage)          echo "payment-gw fails EVERY authorisation   -> use for browser demos; fails on the first click" ;;
    ledger-slow-queries)     echo "ledger-svc adds 800ms of DB time       -> expect root cause: ledger-svc / Postgres" ;;
    inventory-oom)           echo "inventory-svc leaks memory until OOM   -> expect root cause: inventory-svc (OOMKilled)" ;;
    fraud-lag)               echo "fraud-detector stops consuming         -> expect symptom: ConsumerLag_High on orders" ;;
    pricing-cpu)             echo "pricing-engine burns CPU per request   -> expect root cause: pricing-engine (throttled)" ;;
    cart-timeouts)           echo "slow cart + tight checkout timeout     -> expect root cause: cart-service, not checkout-api" ;;
    catalog-cache-miss)      echo "catalog-api bypasses its cache         -> expect load shift onto inventory-svc / Postgres" ;;
    ledger-pool-exhaustion)  echo "ledger-svc leaks DB connections        -> expect root cause: ledger-svc pool exhaustion" ;;
    risk-crash)              echo "risk-model panics on 2% of requests    -> expect root cause: risk-model (CrashLoopBackOff)" ;;
    checkout-latency)        echo "checkout-api adds its own latency      -> control case: cause == symptom" ;;
  esac
}

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

log()  { printf '\033[36m==>\033[0m %s\n' "$*"; }
warn() { printf '\033[33m warn\033[0m %s\n' "$*" >&2; }
die()  { printf '\033[31merror\033[0m %s\n' "$*" >&2; exit 1; }

require_tools() {
  for t in kubectl curl; do
    command -v "$t" >/dev/null 2>&1 || die "$t is required but not installed"
  done
}

# running_pods_for <service>
#
# Every pod of the service, not just one. This matters: most services run with
# replicas: 2, and a fault applied to a single pod is diluted by the Service's
# load balancing — a 35% error rate on one of two pods is ~17.5% overall, with
# half of all requests unaffected. Faults are per-process state held in each
# pod's fault store, so they must be set on every replica.
running_pods_for() {
  local service="$1"
  kubectl -n "$NAMESPACE" get pods \
    -l "app.kubernetes.io/name=${service}" \
    --field-selector=status.phase=Running \
    -o jsonpath='{.items[*].metadata.name}' 2>/dev/null
}

# post_admin_pod <service> <pod> <method> <path> [body]
#
# Port-forwards to one pod's admin port, issues a single request, and tears the
# tunnel down. Slower than holding a tunnel open, but it cannot leave a stray
# background process behind after a demo.
post_admin_pod() {
  local service="$1" pod="$2" method="$3" path="$4" body="${5:-}"
  local port="$LOCAL_PORT"

  kubectl -n "$NAMESPACE" port-forward "pod/${pod}" "${port}:${ADMIN_PORT}" \
    >/dev/null 2>&1 &
  local pf_pid=$!

  local ready=0
  for _ in $(seq 1 40); do
    if curl -fsS --max-time 1 "http://127.0.0.1:${port}/healthz" >/dev/null 2>&1; then
      ready=1
      break
    fi
    sleep 0.25
  done

  local rc=1
  if [[ $ready -ne 1 ]]; then
    warn "${pod}: could not reach the admin port"
  else
    local args=(-fsS --max-time 10 -X "$method" "http://127.0.0.1:${port}${path}")
    if [[ -n "$body" ]]; then
      args+=(-H 'Content-Type: application/json' -d "$body")
    fi
    local out
    if out="$(curl "${args[@]}" 2>&1)"; then
      printf '  %-48s %s\n' "$pod" "$out"
      rc=0
    else
      warn "${pod}: ${out}"
    fi
  fi

  kill "$pf_pid" 2>/dev/null || true
  wait "$pf_pid" 2>/dev/null || true
  return $rc
}

# post_admin <service> <method> <path> [body]
#
# Fans the request out to every running pod of the service.
post_admin() {
  local service="$1" method="$2" path="$3" body="${4:-}"

  local pods
  pods="$(running_pods_for "$service")"
  if [[ -z "$pods" ]]; then
    warn "no running pods for $service in namespace $NAMESPACE — skipping"
    return 0
  fi

  local total=0 ok_count=0
  for pod in $pods; do
    total=$((total + 1))
    if post_admin_pod "$service" "$pod" "$method" "$path" "$body"; then
      ok_count=$((ok_count + 1))
    fi
  done

  # A partial application is worse than none: the fault would be diluted and the
  # demo would show a weaker signal than the scenario describes.
  if [[ "$ok_count" -ne "$total" ]]; then
    warn "$service: applied to ${ok_count}/${total} replicas — the fault is diluted; retry before demoing"
    return 1
  fi
  printf '  \033[32m✓\033[0m %s: applied to all %d replica(s)\n' "$service" "$total"
  return 0
}

# ---------------------------------------------------------------------------
# Commands
# ---------------------------------------------------------------------------

cmd_list() {
  echo "Available scenarios:"
  echo
  for s in "${SCENARIOS[@]}"; do
    printf '  \033[1m%-24s\033[0m %s\n' "$s" "$(scenario_description "$s")"
  done
  echo
  echo "Usage: $0 start <scenario> | stop <scenario> | stop-all | status"
  echo
  echo "Note: error-rate and latency symptoms can take 10-15 minutes to fire in"
  echo "Causely. Raise load first to shorten that: ./scripts/load.sh 100"
}

cmd_start() {
  local name="${1:-}"
  [[ -n "$name" ]] || die "usage: $0 start <scenario>  (see: $0 list)"

  local spec
  spec="$(scenario_spec "$name")" || die "unknown scenario '$name' (see: $0 list)"

  log "starting scenario: $name"
  scenario_description "$name" | sed 's/^/    /'
  echo

  for pair in $spec; do
    local service="${pair%%=*}"
    local patch="${pair#*=}"
    post_admin "$service" POST /admin/faults "$patch"
  done

  echo
  log "scenario '$name' is active. Clear it with: $0 stop $name"
}

cmd_stop() {
  local name="${1:-}"
  [[ -n "$name" ]] || die "usage: $0 stop <scenario>  (see: $0 list)"

  local spec
  spec="$(scenario_spec "$name")" || die "unknown scenario '$name' (see: $0 list)"

  log "stopping scenario: $name"
  for pair in $spec; do
    local service="${pair%%=*}"
    # DELETE clears every fault and releases leaked memory and connections, so
    # the service recovers without a restart.
    post_admin "$service" DELETE /admin/faults
  done
  echo
  log "scenario '$name' cleared"
}

cmd_stop_all() {
  log "clearing faults on every service"
  for service in "${ALL_SERVICES[@]}"; do
    post_admin "$service" DELETE /admin/faults
  done
  echo
  log "all faults cleared"
}

cmd_status() {
  log "active faults by service (empty values mean healthy)"
  for service in "${ALL_SERVICES[@]}"; do
    post_admin "$service" GET /admin/faults
  done
}

main() {
  require_tools
  case "${1:-list}" in
    list)     cmd_list ;;
    start)    shift; cmd_start "$@" ;;
    stop)     shift; cmd_stop "$@" ;;
    stop-all) cmd_stop_all ;;
    status)   cmd_status ;;
    *)        die "unknown command '${1}'. Try: list, start, stop, stop-all, status" ;;
  esac
}

main "$@"
