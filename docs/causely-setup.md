# Causely setup and the trace-ingest contract

This document records what Causely's mediator actually requires from incoming traces. The demo's
instrumentation is shaped to satisfy all of it; this is the reference for anyone changing it, and
the checklist when something is missing from the topology.

## Ingest endpoint

Causely's documented OTLP ingest is **`mediator.causely:4317`, OTLP gRPC, plaintext**. That is
the chart default:

```yaml
otelCollector:
  exporter:
    endpoint: mediator.causely:4317
    protocol: grpc
    insecure: true
```

Both traces and OTLP metrics are accepted on the same gRPC port.

### The namespace is not always `causely`

**On an install where the Causely control plane and the mediators are split, the mediator lives in a
per-environment namespace.** The `causely` namespace then holds only the backend — `analysis`, `api`,
`background`, `gateway`, `ui` — and contains no `mediator` Service at all.

On such an install you will see one `mediator.<environment>:4317` per monitored environment, and
`mediator.causely:4317` — the chart default, and what Causely's docs show for a single-tenant
install — does not resolve at all.

This fails silently in the worst way: the application pods are healthy, the collector accepts spans,
and only the collector's exporter logs show the failure. Always discover the endpoint rather than
assuming it:

```bash
make mediators
```

Verify what the deployed collector is actually using:

```bash
kubectl -n tracey-shop get cm tracey-shop-otel-collector -o yaml | grep -A6 causely
```

Two further notes:

- The mediator Service also exposes `54318`. That is the mediator's **own self-telemetry**
  receiver, not an ingest path for application traces. Some existing Causely demo tooling targets it
  anyway; prefer `4317` gRPC.
- For high trace volumes Causely offers `trace-controller.causely:4317`, which shards by
  `service.name`. The demo sets a stable `service.name` per service, so it works there unchanged.

See [deploy.md](deploy.md) for the full deployment path.

## The one requirement that silently breaks everything

**`k8sattributes` is mandatory in Kubernetes.** The mediator resolves a span's workload in this
order:

1. `k8s.pod.uid`
2. `k8s.namespace.name` + `k8s.pod.name`
3. `container.id`

If none resolve, there is no workload → no Service entity → **the span is discarded**. It is not
logged as an error and it is not partially ingested; the service simply never appears.

`service.name` alone does **not** create an entity while the Kubernetes scraper is active. The
Service entity comes from the k8s scraper's own `Workload → Service` relation; `service.name` is
used for labelling and for shard routing, not for workload resolution.

The chart covers this twice over:

- the bundled collector runs `k8sattributes` with the full extract list, backed by a ClusterRole
  granting `get,list,watch` on pods, namespaces, nodes and replicasets;
- every pod additionally sets `k8s.pod.name`, `k8s.pod.uid`, `k8s.namespace.name`,
  `k8s.node.name` and `k8s.container.name` from the downward API, so traces stay mappable if
  they are ever routed through a collector without the processor.

If you set `otelCollector.enabled=false`, the collector you point at must run `k8sattributes`.

## Span kind determines the edge

Only four span kinds are analysed:

| Kind | Becomes |
|---|---|
| `SERVER` | the service's own latency and error-rate metrics, plus HTTP path / RPC method entities |
| `CLIENT` | a dependency edge to the peer |
| `PRODUCER` | a "Produces" edge to a topic |
| `CONSUMER` | a "Consumes" edge from a topic |

`INTERNAL` spans are ignored, so the chart drops them at the collector
(`otelCollector.filterInternalSpans`) to save ingest volume without losing topology.

Consequence for instrumentation: anything that should appear as a dependency **must** be a
CLIENT/PRODUCER/CONSUMER span carrying peer attributes. An internal span describing an outbound
call contributes nothing.

## Attributes per edge type

| Edge | Required | Also used |
|---|---|---|
| HTTP client | `server.address` (or `url.full`) | `server.port`, `http.request.method`, `http.response.status_code`, `url.path` |
| gRPC client | `rpc.system` **and** a peer address | `rpc.method`, `rpc.service`, `rpc.grpc.status_code`, `server.port` |
| Database | `db.system` and `db.query.text` (or `db.statement`) and `server.address` | `db.namespace`, `db.collection.name`, `db.operation.name` |
| Kafka | `messaging.destination.name` and a broker address | `messaging.system`, `messaging.consumer.group.name`, `messaging.operation.name` |

Peer resolution order for gRPC is `server.address` → `net.peer.name` → `net.sock.peer.addr` →
`net.peer.address` → `net.peer.ip` → `peer.service`. **`peer.service` is a last resort, not a
first-class key** — set `server.address` and `server.port`.

`messaging.consumer.group.name` is what lets consumer lag be attributed to a group rather than
just a topic. Without it the `fraud-lag` scenario cannot be pinned to `fraud-detector`.

### How the demo satisfies this

| Edge | Source |
|---|---|
| HTTP client/server | `otelhttp` v0.62, which emits the stable HTTP semconv by default |
| gRPC client/server | `otelgrpc` v0.62 (`rpc.system`, `rpc.method`, `rpc.grpc.status_code`) |
| Postgres | `otelpgx` (`db.system=postgresql`, `db.query.text`, `db.namespace`, `server.address`) |
| Valkey | `redisotel` (`db.system=redis`, `db.statement`, `server.address`) |
| Kafka | hand-written spans in `internal/transport/kafkax` |

Two places where the demo does extra work rather than trusting the library:

- **`internal/transport/grpcx`** chains a small `stats.Handler` after otelgrpc's. otelgrpc sets
  `server.address` from grpc's *resolved peer*, which in Kubernetes is an IP. Causely can resolve
  either an IP or a hostname, but the hostname path is the well-trodden one — it indexes Services
  by hostname and retries a short name as `<name>.<caller's namespace>`. The extra handler pins
  the DNS target back onto the span so the resolved edge is deterministic.
- **`internal/transport/kafkax`** builds PRODUCER/CONSUMER spans by hand with explicit semconv
  v1.34 attribute names, because messaging semconv has churned and an instrumentation library
  pinned to an older vintage would emit `messaging.destination` instead of
  `messaging.destination.name` — which Causely reads as a different key.

## What gets dropped

Worth knowing, because these look like bugs otherwise:

- **destination port 4317** — so app→collector traffic never becomes an edge. The demo therefore
  keeps every business port off 4317.
- loopback, `127.0.0.1`, `localhost`, `::1`, unix sockets, `169.254.169.254`, port 10250
- health-ish URL paths: `/health`, `/live`, `/ready`, `/metrics`, `/status`, `/debug`, `/info`,
  `/stats` — the demo keeps probes on a separate untraced admin port anyway
- `container.name == "istio-proxy"` spans
- attributes not in the mediator's semantic-convention map are stripped on ingest, so inventing
  custom attribute names achieves nothing

## Naming

- Keep the whole demo in **one namespace**. Causely resolves a short hostname by retrying it as
  `<hostname>.<caller's namespace>`, so cross-namespace calls need a qualified name.
- Never put `causely` in the demo namespace name — spans from a namespace containing that string
  are tagged as Causely-internal.
- Causely names services `<namespace>/<service>`, e.g. `tracey-shop/checkout-api`. That is the
  form to pass to MCP tools like `get_service_summary`.

## The PostgreSQL scraper (separate from traces)

Traces build the service topology. Causely's native PostgreSQL integration is a **different
pipeline**: the mediator connects to the database directly and collects table schemas, lock
monitoring, cache/IO performance and slow queries from `pg_stat_statements`. The chart wires it up
automatically; `make verify-db` checks it.

Three things about it that are not obvious from the docs:

1. **The credentials secret goes in the *mediator's* namespace**, not `causely` and not the
   application's — run `make mediators` to find it. Causely autodiscovers any secret
   labelled `causely.ai/scraper: Postgresql`, which is why this needs no change to the Causely
   release — and is what makes the integration survive a redeploy of this chart.
2. **`host` must be the FQDN Causely's Kubernetes scraper discovers for the Service**
   (`tracey-shop-postgres.tracey-shop.svc.cluster.local`), or the Database entity cannot be linked
   to the workload hosting it.
3. **A failed scraper initialisation is never retried.** The mediator initialises a scraper once per
   discovery event. If the database is not ready at that moment — which is the norm on a fresh
   install, because Helm writes the secret while Postgres is still starting on an empty volume — you
   get this, once, and then silence:

   ```
   auto discovered scraper {"secret":"tracey-shop-postgres-credentials",...}
   scraper initialization failed {... "failed to ping PostgreSQL database:
       pq: password authentication failed for user "causely_monitor" (28P01)"}
   ```

   The chart's post-install Job exists solely to close that race: it blocks until it can
   authenticate, then annotates the secret to re-fire discovery. Check it worked with:

   ```bash
   kubectl -n causely logs deploy/mediator --all-containers --tail=2000 \
     | grep tracey-shop-postgres-credentials
   ```

   A healthy result ends in `starting event listener`, with no `initialization failed`.

### One known cosmetic artifact

Causely may show **two** Database entities for the same database: the scraper-owned one named
`shop`, and an older trace-derived one named `unknown`. The `unknown` one appears when spans reach
Causely without `db.namespace` (or `db.name`) — it reads those to name the entity, and entity ids are
content-derived hashes, so an entity cannot be renamed afterwards, only aged out.

`internal/transport/pgxx` now asserts both attributes explicitly, so new spans no longer produce it.
An `unknown` entity created before that fix persists until Causely ages it out.

## Confirming it works

```bash
./scripts/verify-traces.sh --upgrade
```

That asserts each requirement above against the collector's own detailed output. Then, from the
Causely side:

| Check | Expectation |
|---|---|
| `get_entities(namespace_names=["tracey-shop"])` | all 15 services present as Service entities |
| `get_topology` | five layers, gRPC and HTTP edges, Postgres and Valkey database entities, Produces/Consumes edges on all three topics |
| `get_service_summary(service="tracey-shop/checkout-api")` | healthy, SLOs satisfied |
| `get_symptoms` for the namespace | **empty** — a clean baseline is the whole point |

If a symptom fires on a clean baseline, it is almost certainly `MemoryUtilization_High` or
`CPUThrottled_High` from limits set too close to steady-state usage. Raise the relevant
`resources.limits` in values; the chart's defaults deliberately leave 3–5× headroom for exactly
this reason.

## Keeping the baseline clean

Design choices in the demo that exist to avoid false positives:

- generous resource limits on every workload, so utilisation sits near 15–20% at steady state
- deterministic seed data and product ids, so the load generator never requests something absent
- `actionCheckout` creates its own cart and adds items before checking out, so checkout never
  sees an empty cart and never returns a 4xx
- Valkey runs bounded with `allkeys-lru`, so the cache cannot grow into a memory symptom
- `terminationGracePeriodSeconds: 30` and generous readiness `failureThreshold`, so rollouts and
  cold starts do not register as errors
- a 15-second startup delay in the load generator, so the first requests of a fresh install do
  not fail while dependencies are still becoming ready
