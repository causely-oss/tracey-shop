#!/usr/bin/env bash
#
# Verify the Causely PostgreSQL integration.
#
# Runs the checks from https://docs.causely.ai/telemetry-sources/postgresql/
# against the monitoring user Causely actually connects as — not the application
# user — plus the Kubernetes-side wiring that makes the scraper find it.
#
# Usage:
#   scripts/verify-db-integration.sh

set -euo pipefail

NAMESPACE="${NAMESPACE:-tracey-shop}"
RELEASE="${RELEASE:-tracey-shop}"

pass=0
fail=0

log()  { printf '\033[36m==>\033[0m %s\n' "$*"; }
ok()   { printf '  \033[32mPASS\033[0m %s\n' "$*"; pass=$((pass+1)); }
bad()  { printf '  \033[31mFAIL\033[0m %s\n' "$*"; fail=$((fail+1)); }
info() { printf '       %s\n' "$*"; }
die()  { printf '\033[31merror\033[0m %s\n' "$*" >&2; exit 1; }

command -v kubectl >/dev/null || die "kubectl is required"

PG_STS="${RELEASE}-postgres"
kubectl -n "$NAMESPACE" get "statefulset/$PG_STS" >/dev/null 2>&1 \
  || die "no statefulset $PG_STS in namespace $NAMESPACE (external Postgres? then check it by hand)"

# ---------------------------------------------------------------------------
# Resolve what the chart configured
# ---------------------------------------------------------------------------

log "resolving the integration secret"

# Find the secret by its scraper label, wherever the chart put it.
read -r SECRET_NS SECRET_NAME <<<"$(
  kubectl get secrets -A -l 'causely.ai/scraper=Postgresql' \
    -o jsonpath='{range .items[*]}{.metadata.namespace}{" "}{.metadata.name}{"\n"}{end}' 2>/dev/null \
    | grep -F "${RELEASE}" | head -1
)" || true

if [[ -z "${SECRET_NAME:-}" ]]; then
  bad "no Secret labelled causely.ai/scraper=Postgresql for release ${RELEASE}"
  info "is causelyIntegrations.postgres.enabled=true? check: kubectl get secrets -A -l causely.ai/scraper=Postgresql"
  exit 1
fi
ok "found ${SECRET_NS}/${SECRET_NAME} labelled causely.ai/scraper=Postgresql"

secret_val() {
  kubectl -n "$SECRET_NS" get secret "$SECRET_NAME" -o jsonpath="{.data.$1}" 2>/dev/null | base64 -d 2>/dev/null
}

DB_USER="$(secret_val username)"
DB_PASS="$(secret_val password)"
DB_HOST="$(secret_val host)"
DB_NAME="$(secret_val database)"
DB_PORT="$(secret_val port)"

info "user=${DB_USER} host=${DB_HOST}:${DB_PORT} database=${DB_NAME}"

# ---------------------------------------------------------------------------
# The secret must point at the FQDN Causely discovers for the Service
# ---------------------------------------------------------------------------

log "checking the host matches what Causely's Kubernetes scraper discovers"

EXPECTED_HOST="${PG_STS}.${NAMESPACE}.svc.cluster.local"
if [[ "$DB_HOST" == "$EXPECTED_HOST" ]]; then
  ok "host is the in-cluster FQDN, so the Database entity can be linked to the Service"
else
  info "host is ${DB_HOST}, not ${EXPECTED_HOST}"
  info "that is correct for an external database; for the bundled one it must be the FQDN"
fi

if ! kubectl -n "$SECRET_NS" get secret "$SECRET_NAME" >/dev/null 2>&1; then
  bad "secret is not readable"
fi

# ---------------------------------------------------------------------------
# Server-side prerequisites
# ---------------------------------------------------------------------------

# psql as the SUPERUSER, to check server configuration.
psql_admin() {
  kubectl -n "$NAMESPACE" exec "statefulset/$PG_STS" -- \
    psql -U "$(kubectl -n "$NAMESPACE" get secret "${RELEASE}-postgres" \
      -o jsonpath='{.data.POSTGRES_USER}' | base64 -d)" \
    -d "$DB_NAME" -tAc "$1" 2>/dev/null | tr -d '\r'
}

# psql as the MONITORING user, which is what actually matters — the scraper
# connects as this role, so its permissions are what must be verified.
psql_monitor() {
  kubectl -n "$NAMESPACE" exec "statefulset/$PG_STS" -- \
    env PGPASSWORD="$DB_PASS" psql -U "$DB_USER" -h 127.0.0.1 -d "$DB_NAME" -tAc "$1" 2>&1 | tr -d '\r'
}

log "checking server configuration"

spl="$(psql_admin "SELECT setting FROM pg_settings WHERE name='shared_preload_libraries';")"
if [[ "$spl" == *pg_stat_statements* ]]; then
  ok "shared_preload_libraries includes pg_stat_statements"
else
  bad "shared_preload_libraries is '${spl}' — pg_stat_statements is not preloaded"
  info "this requires a server restart; it is set in templates/postgres.yaml"
fi

ext="$(psql_admin "SELECT EXISTS(SELECT 1 FROM pg_extension WHERE extname='pg_stat_statements');")"
if [[ "$ext" == "t" ]]; then
  ok "pg_stat_statements extension is created"
else
  bad "pg_stat_statements extension is missing — slow-query analysis will be empty"
  info "created by templates/postgres-init-configmap.yaml on a fresh PGDATA"
fi

log "checking the monitoring role exists"

role="$(psql_admin "SELECT EXISTS(SELECT 1 FROM pg_roles WHERE rolname='${DB_USER}');")"
if [[ "$role" == "t" ]]; then
  ok "role ${DB_USER} exists"
else
  bad "role ${DB_USER} does not exist"
  info "the init SQL only runs when PGDATA is empty; with persistence enabled on a"
  info "pre-existing volume you must create it by hand — see docs/causely-setup.md"
  exit 1
fi

# ---------------------------------------------------------------------------
# The docs' quick check, run as the monitoring user
# ---------------------------------------------------------------------------

log "running the documented quick check as ${DB_USER}"

quick="$(psql_monitor "
SELECT
    CASE WHEN EXISTS (SELECT 1 FROM pg_stat_user_tables LIMIT 1)
         THEN 'PASS' ELSE 'FAIL' END AS perm_check,
    CASE WHEN EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'pg_stat_statements')
         THEN 'PASS' ELSE 'FAIL' END AS ext_check,
    CASE WHEN EXISTS (SELECT 1 FROM pg_stat_statements LIMIT 1)
         THEN 'PASS' ELSE 'FAIL' END AS access_check;")"

if [[ "$quick" == *"authentication failed"* || "$quick" == *"does not exist"* ]]; then
  bad "could not connect as ${DB_USER}: ${quick}"
  exit 1
fi

IFS='|' read -r perm ext_c access <<<"$quick"
[[ "$perm" == "PASS" ]]   && ok "perm_check   — can read pg_stat_user_tables (pg_read_all_stats)" \
                          || bad "perm_check   — cannot read pg_stat_user_tables; grant pg_read_all_stats"
[[ "$ext_c" == "PASS" ]]  && ok "ext_check    — pg_stat_statements is installed" \
                          || bad "ext_check    — pg_stat_statements is missing"
[[ "$access" == "PASS" ]] && ok "access_check — pg_stat_statements is loaded and queryable" \
                          || bad "access_check — pg_stat_statements not queryable"

log "checking schema and table access"

if [[ "$(psql_monitor "SELECT 1 FROM information_schema.columns LIMIT 1;")" == "1" ]]; then
  ok "can read information_schema (table and column metadata)"
else
  bad "cannot read information_schema"
fi

tables="$(psql_monitor "SELECT count(*) FROM information_schema.tables WHERE table_schema='public';")"
if [[ "${tables:-0}" -gt 0 ]]; then
  ok "sees ${tables} tables in the public schema"
else
  bad "sees no tables in the public schema"
fi

if [[ "$(psql_monitor "SELECT count(*) >= 0 FROM products;")" == "t" ]]; then
  ok "can SELECT from an application table (default privileges are working)"
else
  info "cannot SELECT from application tables; row counts may be unavailable"
  info "ALTER DEFAULT PRIVILEGES only covers tables created after the init SQL ran"
fi

log "top slow queries visible to Causely"

slow="$(psql_monitor "
SELECT substr(regexp_replace(query, '\s+', ' ', 'g'), 1, 60) || ' | calls=' || calls
FROM pg_stat_statements
WHERE query NOT LIKE '%pg_stat_statements%'
ORDER BY total_exec_time DESC LIMIT 3;")"
if [[ -n "$slow" ]]; then
  ok "pg_stat_statements has data"
  while IFS= read -r line; do [[ -n "$line" ]] && info "$line"; done <<<"$slow"
else
  info "pg_stat_statements is empty; normal on a fresh instance, it fills as traffic runs"
fi

echo
printf '\033[1m%s\033[0m\n' "Summary: ${pass} passed, ${fail} failed"
if [[ "$fail" -gt 0 ]]; then
  exit 1
fi
cat <<EOF
The PostgreSQL integration is configured. Causely should show:
  - a Database entity for ${DB_NAME} in the topology, linked to ${PG_STS}
  - Table entities with schemas, and slow queries via get_slow_queries
It can take a few minutes for the scraper to pick up a newly labelled secret.
EOF
