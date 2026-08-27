{{/*
Chart name and fullname helpers.
*/}}
{{- define "tracey-shop.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "tracey-shop.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- include "tracey-shop.name" . -}}
{{- end -}}
{{- end -}}

{{- define "tracey-shop.labels" -}}
app.kubernetes.io/part-of: {{ include "tracey-shop.name" . }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" }}
{{- end -}}

{{- define "tracey-shop.serviceAccountName" -}}
{{- if .Values.serviceAccount.create -}}
{{- default (include "tracey-shop.fullname" .) .Values.serviceAccount.name -}}
{{- else -}}
{{- default "default" .Values.serviceAccount.name -}}
{{- end -}}
{{- end -}}

{{- define "tracey-shop.collectorName" -}}
{{- printf "%s-otel-collector" (include "tracey-shop.fullname" .) -}}
{{- end -}}

{{/*
The OTLP endpoint the application pods export to. Prefers the bundled
collector; falls back to .Values.otel.endpoint.
*/}}
{{- define "tracey-shop.otlpEndpoint" -}}
{{- if .Values.otelCollector.enabled -}}
{{- printf "%s:%d" (include "tracey-shop.collectorName" .) (int .Values.otelCollector.ports.grpc) -}}
{{- else -}}
{{- .Values.otel.endpoint -}}
{{- end -}}
{{- end -}}

{{/*
Backend endpoints. Each honours an `external` override so the bundled backend
can be swapped for a real one.
*/}}
{{- define "tracey-shop.postgresHost" -}}
{{- if .Values.backends.postgres.external -}}
{{- .Values.backends.postgres.external -}}
{{- else -}}
{{- printf "%s-postgres:5432" (include "tracey-shop.fullname" .) -}}
{{- end -}}
{{- end -}}

{{- define "tracey-shop.postgresDSN" -}}
{{- $pg := .Values.backends.postgres -}}
{{- printf "postgres://%s:%s@%s/%s?sslmode=disable" $pg.username $pg.password (include "tracey-shop.postgresHost" .) $pg.database -}}
{{- end -}}

{{- define "tracey-shop.valkeyAddr" -}}
{{- if .Values.backends.valkey.external -}}
{{- .Values.backends.valkey.external -}}
{{- else -}}
{{- printf "%s-valkey:6379" (include "tracey-shop.fullname" .) -}}
{{- end -}}
{{- end -}}

{{- define "tracey-shop.kafkaBrokers" -}}
{{- if .Values.backends.kafka.external -}}
{{- .Values.backends.kafka.external -}}
{{- else -}}
{{- printf "%s-kafka:9092" (include "tracey-shop.fullname" .) -}}
{{- end -}}
{{- end -}}

{{/*
Environment shared by every application pod.

Dependency endpoints are injected into all roles rather than only the ones that
use them: connections are opened lazily, so an unused endpoint costs nothing and
the template stays a single flat block.

The downward-API k8s.* attributes matter — Causely resolves a span's workload
from k8s.pod.uid or k8s.namespace.name + k8s.pod.name, and drops spans it cannot
map. The collector's k8sattributes processor is the primary source; these are the
belt-and-braces copy carried by the SDK itself.
*/}}
{{- define "tracey-shop.commonEnv" -}}
{{- $root := .root -}}
{{- $fullname := include "tracey-shop.fullname" $root -}}
{{- $svcs := $root.Values.services -}}
- name: POD_NAME
  valueFrom:
    fieldRef:
      fieldPath: metadata.name
- name: POD_UID
  valueFrom:
    fieldRef:
      fieldPath: metadata.uid
- name: POD_NAMESPACE
  valueFrom:
    fieldRef:
      fieldPath: metadata.namespace
- name: NODE_NAME
  valueFrom:
    fieldRef:
      fieldPath: spec.nodeName
- name: CONTAINER_NAME
  value: shopd
- name: LOG_LEVEL
  value: {{ $root.Values.global.logLevel | quote }}
- name: DEPLOY_ENV
  value: {{ $root.Values.global.env | quote }}
- name: SERVICE_VERSION
  value: {{ $root.Chart.AppVersion | quote }}
- name: REQUEST_TIMEOUT
  value: {{ $root.Values.global.requestTimeout | quote }}
{{/*
Order-intake backpressure. Only checkout-api acts on these, but they are
injected everywhere for the same reason the dependency endpoints are: one flat
env block is easier to reason about than per-role conditionals.
*/}}
- name: BACKPRESSURE_ENABLED
  value: {{ $root.Values.orderIntakeBackpressure.enabled | quote }}
- name: BACKPRESSURE_LAG_THRESHOLD
  value: {{ $root.Values.orderIntakeBackpressure.lagThreshold | quote }}
- name: BACKPRESSURE_REJECT_RATE
  value: {{ $root.Values.orderIntakeBackpressure.rejectRate | quote }}
- name: BACKPRESSURE_LATENCY
  value: {{ $root.Values.orderIntakeBackpressure.latency | quote }}
- name: BACKPRESSURE_POLL_INTERVAL
  value: {{ $root.Values.orderIntakeBackpressure.pollInterval | quote }}
{{/*
The browser storefront. Only storefront-bff serves the UI, but this follows the
same flat-env convention as everything above.
*/}}
- name: WEB_UI_ENABLED
  value: {{ $root.Values.storefrontWeb.enabled | quote }}
- name: ADMIN_ADDR
  value: {{ printf ":%d" (int $root.Values.adminPort) | quote }}
- name: OTEL_EXPORTER_OTLP_ENDPOINT
  value: {{ include "tracey-shop.otlpEndpoint" $root | quote }}
- name: OTEL_TRACES_SAMPLER_RATIO
  value: {{ $root.Values.otel.sampling | quote }}
- name: POSTGRES_DSN
  value: {{ include "tracey-shop.postgresDSN" $root | quote }}
- name: REDIS_ADDR
  value: {{ include "tracey-shop.valkeyAddr" $root | quote }}
- name: KAFKA_BROKERS
  value: {{ include "tracey-shop.kafkaBrokers" $root | quote }}
- name: TOPIC_ORDERS
  value: {{ $root.Values.backends.kafka.topics.orders | quote }}
- name: TOPIC_LEDGER_EVENTS
  value: {{ $root.Values.backends.kafka.topics.ledgerEvents | quote }}
- name: TOPIC_NOTIFICATIONS
  value: {{ $root.Values.backends.kafka.topics.notifications | quote }}
- name: GROUP_FRAUD
  value: {{ $root.Values.backends.kafka.consumerGroups.fraud | quote }}
- name: GROUP_NOTIFICATIONS
  value: {{ $root.Values.backends.kafka.consumerGroups.notifications | quote }}
{{/*
Short in-namespace DNS names on purpose: Causely's resolver retries an
unqualified hostname as <name>.<caller's namespace>, so keeping the whole demo in
one namespace makes every dependency edge resolvable.
*/}}
- name: CATALOG_ADDR
  value: {{ printf "%s-catalog-api:%d" $fullname (int (index $svcs "catalog-api").port) | quote }}
- name: CHECKOUT_ADDR
  value: {{ printf "%s-checkout-api:%d" $fullname (int (index $svcs "checkout-api").port) | quote }}
- name: PRICING_ADDR
  value: {{ printf "%s-pricing-engine:%d" $fullname (int (index $svcs "pricing-engine").port) | quote }}
- name: INVENTORY_ADDR
  value: {{ printf "%s-inventory-svc:%d" $fullname (int (index $svcs "inventory-svc").port) | quote }}
- name: PAYMENT_ADDR
  value: {{ printf "%s-payment-gw:%d" $fullname (int (index $svcs "payment-gw").port) | quote }}
- name: LEDGER_ADDR
  value: {{ printf "%s-ledger-svc:%d" $fullname (int (index $svcs "ledger-svc").port) | quote }}
- name: RISK_ADDR
  value: {{ printf "%s-risk-model:%d" $fullname (int (index $svcs "risk-model").port) | quote }}
- name: CART_URL
  value: {{ printf "http://%s-cart-service:%d" $fullname (int (index $svcs "cart-service").port) | quote }}
- name: SHIPPING_URL
  value: {{ printf "http://%s-shipping-quote:%d" $fullname (int (index $svcs "shipping-quote").port) | quote }}
- name: STRIPE_URL
  value: {{ printf "http://%s-stripe-sim:%d" $fullname (int (index $svcs "stripe-sim").port) | quote }}
- name: CARRIER_URL
  value: {{ printf "http://%s-carrier-sim:%d" $fullname (int (index $svcs "carrier-sim").port) | quote }}
- name: EMAIL_URL
  value: {{ printf "http://%s-email-sim:%d" $fullname (int (index $svcs "email-sim").port) | quote }}
{{- end -}}
