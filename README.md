# Tracey Shop

[![CI](https://github.com/causely-oss/tracey-shop/actions/workflows/ci.yaml/badge.svg)](https://github.com/causely-oss/tracey-shop/actions/workflows/ci.yaml)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)
[![Image](https://img.shields.io/badge/ghcr.io-causely--oss%2Ftracey--shop-blue)](https://github.com/causely-oss/tracey-shop/pkgs/container/tracey-shop)

A realistic e-commerce demo application for showcasing **Causely's ability to pinpoint the root
cause of a problem** in a multi-service Kubernetes environment.

Fifteen services across five layers, communicating over HTTP, gRPC, Postgres, Valkey and Kafka,
fully instrumented with OpenTelemetry traces, deployed by one Helm chart, with a rate-adjustable
load generator and eleven fault scenarios you can start and stop mid-demo without a restart.

```
helm install → clean, error-free baseline
scenario.sh start payment-errors → Causely names payment-gw
scenario.sh stop payment-errors  → diagnosis clears
```

## Quick start

The container image is published, so there is nothing to build.

```bash
# 1. A cluster. Skip if you already have one.
./scripts/kind-up.sh

# 2. Confirm your Causely mediator endpoint. Usually mediator.causely:4317.
make mediators

# 3. Install. The image is pulled from ghcr.io/causely-oss/tracey-shop.
helm upgrade --install tracey-shop deploy/tracey-shop \
  -n tracey-shop --create-namespace \
  -f deploy/tracey-shop/values-kind.yaml \
  --wait --timeout 15m \
  --set otelCollector.exporter.endpoint=mediator.causely:4317  # update based on step 2

# 4. Watch it settle. Every pod should reach Running with zero restarts.
make status

# 5. Confirm the traces carry what Causely needs. This will take a couple of minutes.
./scripts/verify-traces.sh --upgrade
```

Then open the shop in a browser:

```bash
make port-forward
open http://localhost:8080          # or curl -s localhost:8080/api/products | jq
```

There is a real browser storefront — a dependency-free single-page shop embedded in the binary and
served by `storefront-bff`. It is the only user-facing surface the demo has, and the only place an
injected fault becomes something a person watching can actually see.

**No Causely in this cluster yet?** Add `--set causelyIntegrations.postgres.enabled=false`. The
PostgreSQL integration writes a Secret into the mediator's namespace, so without one `helm install`
fails with `namespaces "causely" not found`. Everything else — all 15 services, all 11 scenarios,
the storefront — still runs.

**Hacking on the code?** `make kind-load && make deploy` builds locally, side-loads into kind and
deploys that image instead.

For a real cluster, see **[docs/deploy.md](docs/deploy.md)**.

## Topology

Five service hops deep on the synchronous path, plus an asynchronous branch through Kafka.

```
browser ──┐                                                               ← real user (QM)
web-client ┴HTTP─▶ storefront-bff :8080                                   ← layer 1 (edge)
                   (also serves the storefront UI at /)
                       │
        ┌──────────────┼───────────────┬────────────────┬──────────────┐
        │gRPC          │HTTP           │gRPC            │              │HTTP
        ▼              ▼               ▼                │              ▼
   catalog-api    cart-service    checkout-api ─────────┘        ai-assistant :8088
     :9001           :8081            :9002                             │HTTP  ← layer 2
                                                                        ▼
                                                              model-gateway :8089
                                                                (gen_ai.* CLIENT span)
        │ │             │              │ │ │ │
   gRPC │ │Redis   Redis│         gRPC │ │ │ └──HTTP──▶ shipping-quote :8082 ← layer 3
        ▼ └──▶(valkey)  └──▶(valkey)   │ │ │                  │HTTP
   inventory-svc :9003                 │ │ │                  ▼
        │Postgres                      │ │ │            carrier-sim :8085
        ▼                              │ │ └──gRPC──▶ pricing-engine :9004
   (postgres)                          │ │                 │Postgres │Redis
                                       │ │                 ▼         ▼
                                       │ └──gRPC──▶ payment-gw :9005           ← layer 3
                                       │                │            │HTTP
                                       │           gRPC ▼            ▼
                                       │        ledger-svc :9006  stripe-sim :8086 ← layer 4
                                       │           │Postgres  │Kafka: ledger.events
                                       │
                                       └──Kafka PRODUCE topic "orders"
                                                   │
                                                   ▼ CONSUME (group fraud-workers)
                                            fraud-detector                       ← layer 4
                                                   │gRPC
                                                   ▼
                                            risk-model :9007 ──Redis──▶(valkey)  ← layer 5
                                                   │
                                                   └─Kafka PRODUCE "notifications"
                                                             │
                                                             ▼ CONSUME
                                                    notification-worker          ← layer 5
                                                             │HTTP
                                                             ▼
                                                       email-sim :8087
```

Deepest synchronous chain, five services and a database:
`storefront-bff → checkout-api → payment-gw → ledger-svc → postgres`

Deepest asynchronous chain:
`checkout-api → Kafka(orders) → fraud-detector → risk-model → Kafka(notifications) → notification-worker → email-sim`

`stripe-sim`, `carrier-sim` and `email-sim` stand in for third-party providers. They run the
same `partner-sim` implementation under three different service names, so the graph has
realistic leaf dependencies with no internet access required.

`ai-assistant` is the genAI branch: it answers product questions by calling an LLM and emits
GenAI OpenTelemetry spans, which Causely turns into `AIModel` and `AIModelAccess` entities with
their own latency, error-rate and rate-limit symptoms. `model-gateway` is the bundled provider —
an OpenAI-compatible endpoint, so the default install needs no API key, no egress and costs
nothing. Point it at a real provider with one values change. Traffic is **off by default**; see
[docs/genai.md](docs/genai.md).

See [docs/topology.md](docs/topology.md) for the per-service protocol and dependency table.

## Controlling load

The load generator drives only `storefront-bff`, so every request walks a real path through the
graph. Change the rate live — no `helm upgrade`, no restart:

```bash
./scripts/load.sh          # show current rate
./scripts/load.sh 80       # 80 rps
./scripts/load.sh 0        # pause
```

GenAI traffic is paced separately, in **absolute** requests per second, so raising shop load
never multiplies spend against a real LLM provider. It starts at 0:

```bash
./scripts/genai.sh 0.5     # ~1 inference every 2s
./scripts/genai.sh 0       # off
```

Or set the defaults at install time:

```yaml
loadgen:
  rps: 20
  concurrency: 16
  mix:                     # relative weights
    browse: 50
    search: 20
    viewProduct: 15
    addToCart: 10
    checkout: 5            # the full five-layer journey
```

## Fault scenarios

Everything ships **off**. `helm install` gives a clean, error-free baseline — which is the
prerequisite for a credible demo, since Causely should find nothing until you break something.

```bash
./scripts/scenario.sh list
./scripts/scenario.sh start ledger-slow-queries
./scripts/scenario.sh stop  ledger-slow-queries
./scripts/scenario.sh stop-all
```

| Scenario | Injected at | What Causely should conclude |
|---|---|---|
| `payment-errors` | payment-gw, 35% errors | payment-gw — not the three layers above it that also show errors |
| `payment-outage` | payment-gw, 100% errors | same, but fails on the first click — use this when a human is demoing in the browser |
| `ledger-slow-queries` | ledger-svc, 800ms DB time | ledger-svc / Postgres at the bottom of the deepest chain, with no errors anywhere |
| `inventory-oom` | inventory-svc memory leak | inventory-svc, OOMKilled and restarting |
| `fraud-lag` | fraud-detector stops consuming | **Urgent** `Slow Consumer` — the backlog trips checkout-api's order-intake backpressure, so checkout and storefront start failing three hops from the cause |
| `pricing-cpu` | pricing-engine, 40ms CPU burn | pricing-engine, CPU throttled |
| `cart-timeouts` | slow cart + tight checkout timeout | cart-service, though checkout-api is where the errors appear |
| `catalog-cache-miss` | catalog-api bypasses Valkey | load shift onto inventory-svc and Postgres, still no errors |
| `ledger-pool-exhaustion` | ledger-svc leaks connections | ledger-svc, connection-pool exhaustion |
| `risk-crash` | risk-model panics on 2% | risk-model, CrashLoopBackOff |
| `checkout-latency` | checkout-api's own latency | control case: cause and symptom are the same service |
| `ai-model-malfunction` | LLM provider fails 50% of inferences | **AIModel Malfunction** on `mock-small-1/chat` — the failing entity is a model, not a service |

Each scenario also emits a **matching WARN/ERROR log line**, because Causely builds its root-cause
*description* from container logs, not only from metric symptoms — without one the description is
generic. The messages are deliberately domain-plausible (`payment authorization rejected by
acquirer: no auth code returned`, `ledger connection pool exhausted, journal writes are queueing`)
and never mention faults or injection, so the RCA reads as a real incident rather than a staged one.
A test enforces that. See
[docs/scenarios.md](docs/scenarios.md#log-evidence) for the full table.

Full detail, including expected symptoms and how long each takes to fire, is in
[docs/scenarios.md](docs/scenarios.md).

Faults are applied by POSTing to each pod's admin port, which is deliberately **not**
trace-instrumented so scenario tooling never appears as a dependency edge. Because faults are
per-process state, `scenario.sh` applies them to **every replica** and prints
`✓ <service>: applied to all N replica(s)` — if that count is short, retry, because a fault on one
of two pods is halved by load balancing.

> **Match the load mix to the scenario.** The default mix is only 5% checkout, so a 35% failure at
> `payment-gw` shows up as under 2% error rate at the storefront — possibly too thin for a symptom
> to fire. For checkout-path scenarios, weight the mix toward checkout first; see
> [docs/scenarios.md](docs/scenarios.md#match-the-load-mix-to-the-scenario).

## Causely wiring

The chart ships an OTel Collector with the pipeline Causely requires, and exports to the
mediator:

```
application pods ──OTLP gRPC──▶ tracey-shop-otel-collector:4317
                                  ├─ k8sattributes   (REQUIRED — see below)
                                  ├─ filter          (drop INTERNAL spans)
                                  └─ batch
                                        └──OTLP gRPC──▶ mediator.causely:4317
```

> **That collector must run the `k8sattributes` processor.** Causely resolves a span's workload
> from `k8s.pod.uid`, or `k8s.namespace.name` + `k8s.pod.name`, or `container.id`. If none
> resolve, the span is **silently dropped** and the service never appears in the topology. This
> is the single most common reason a demo app is invisible in Causely. The app also sets those
> attributes from the downward API as a fallback.

**Confirm the mediator endpoint.** `mediator.causely:4317` is the chart default and is where a
standard install puts it. A wrong endpoint fails silently — the pods stay healthy and only the
collector's exporter logs complain — so it is worth ten seconds:

```bash
make mediators      # lists every mediator.<namespace>:4317 in the current cluster
```

Repoint at any time:

```bash
helm upgrade tracey-shop deploy/tracey-shop -n tracey-shop --reuse-values \
  --set otelCollector.exporter.endpoint=mediator.<namespace>:4317 \
  --set otelCollector.exporter.protocol=grpc      # or http
```

To use a collector you already run instead:

```bash
--set otelCollector.enabled=false --set otel.endpoint=my-collector.observability:4317
```

[docs/causely-setup.md](docs/causely-setup.md) covers the full ingest contract — which span
attributes drive which topology edges, and what gets filtered.

### Kafka consumer-lag metrics

**Traces cannot carry consumer lag.** They create the Topic and `ConsumerTopicAccess` entities and
the labels Causely matches on, but the `Lag` attribute itself only ever comes from a metric source.
Without one, the `fraud-lag` scenario piles up real lag in Kafka and **Causely stays completely
silent**.

The chart therefore deploys a `kafka-exporter` plus a ServiceMonitor, publishing
`kafka_consumergroup_lag` — exactly what Causely's shipped Prometheus config already queries, so
nothing needs changing in the Causely release. Verify every hop with `make verify-lag`.

The symptom is **`Consumer Lag`**, it lands on the **`ConsumerTopicAccess`** entity rather than the
Topic, and it needs **~6 minutes** to fire. Full detail, including why it needs checkout-api's
backpressure to become Urgent, is in
[docs/scenarios.md](docs/scenarios.md#why-this-one-needs-backpressure-to-be-urgent).

### PostgreSQL integration

Traces give Causely the service topology. Causely's **native PostgreSQL scraper** adds the database
as a first-class entity, with table schemas, lock monitoring and slow-query analysis from
`pg_stat_statements`. The chart sets it up automatically — it comes up with `helm install`, is
re-established by every `helm upgrade`, and needs no edit to the Causely release. Verify it with
`make verify-db`.

Two things worth knowing: the credentials Secret is created in the **mediator's** namespace
(`causelyIntegrations.postgres.mediatorNamespace`, default `causely`), and a post-install Job exists
because the mediator initialises a scraper **once** per discovery event and never retries a failed
one. See [docs/causely-setup.md](docs/causely-setup.md#the-postgresql-scraper-separate-from-traces).

## Deploy to a real Kubernetes cluster

```bash
make mediators
make deploy-cloud MEDIATOR=mediator.causely:4317
```

The published image is multi-arch (amd64 + arm64), so nothing needs building. To publish your own,
`make push REGISTRY=... IMAGE_NAME=...`.

**[docs/deploy.md](docs/deploy.md)** has the full path: cluster requirements, what
`values-cloud.yaml` changes, using a collector you already run, and the kubeconfig/registry
commands for EKS, GKE and AKS.

## Verifying before you blame the platform

`scripts/verify-traces.sh` samples the collector's own output and asserts every attribute the
mediator depends on: resource `k8s.*`, `server.address` on CLIENT spans, `rpc.system.name` on gRPC,
`db.system` + `db.query.text` on database spans, and
`messaging.destination.name` + `messaging.consumer.group.name` on Kafka spans. It also flags
exporter errors.

```bash
./scripts/verify-traces.sh --upgrade --seconds 90
```

A dropped span is invisible from both sides, so run this first when something is missing.

## Architecture

One Go module, one image, one Dockerfile. The `ROLE` environment variable selects which service
the container runs; Helm renders each role as its own Deployment and Service, so Causely still
sees seventeen genuinely distinct application services — plus the load generator's own
`web-client` — each with its own `service.name`.

```
cmd/shopd/main.go              ROLE dispatch
internal/
  config/                      env-driven configuration
  obs/                         OTel setup, resource attributes, logging
  faults/                      fault store and injectors
  genai/                       LLM provider client + GenAI semconv spans
  admin/                       health probes + fault API (never traced)
  transport/
    httpx/ grpcx/              instrumented servers and clients
    kafkax/                    hand-rolled PRODUCER/CONSUMER spans, W3C headers
    pgxx/ redisx/              traced Postgres and Valkey
  store/                       Postgres schema and seed data
  domain/                      shared types
  services/<role>/             one package per service
proto/shop/v1/shop.proto       gRPC contracts (generated code checked in)
deploy/tracey-shop/            Helm chart
scripts/                       kind-up, scenario, load, genai, verify-traces
```

Adding a service is one values block plus one package registered in `cmd/shopd/main.go`.

Kafka spans are constructed by hand rather than via an instrumentation library, because
Causely's messaging analysis needs a precise attribute set and library semconv vintages drift.
For the same reason `grpcx` chains a small stats handler that pins `server.address` to the
Service DNS name — otelgrpc otherwise overwrites it with the resolved peer IP.

## Common tasks

```bash
make help            # list every target
make lint            # go vet + helm lint
make test            # unit tests, incl. chart/code drift checks
make template        # render the chart as `make deploy` would install it
make redeploy        # rebuild, reload into kind, upgrade, restart
make mediators       # list mediator endpoints in the current cluster
make collector-logs  # tail the collector
make status          # pods and restart counts
make undeploy        # remove everything
```

For a real cluster: `make push`, `make deploy-cloud`, `make template-cloud`.

## Requirements

- Helm 3+ and kubectl. That is all you need to run the demo — the image is published.
- kind for a local cluster, or any Kubernetes cluster
- ~6GB of RAM for the cluster: about 20 pods, plus whatever Causely needs alongside
- To build it: Go 1.25+ and Docker. To regenerate the gRPC stubs: `protoc` plus `make proto-tools`

Everything is self-contained — Postgres, Valkey and Kafka are bundled as single-replica
workloads with no persistence by default. Point at existing ones with
`backends.<name>.external`.

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) — how to add a service, how to add a fault scenario, and the
conventions the tests enforce.

## License

[Apache 2.0](LICENSE).
