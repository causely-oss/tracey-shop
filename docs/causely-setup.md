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

### Confirm the namespace

`causely` is where a standard install puts the mediator, and the chart assumes it. If your cluster
differs, the failure is silent in the worst way: the application pods are healthy, the collector
accepts spans, and only the collector's exporter logs show it. So check rather than assume:

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
| gRPC client | `rpc.system.name` **and** a peer address | `rpc.method` (fully qualified), `rpc.response.status_code`, `server.port` |

> **`rpc.system` was renamed to `rpc.system.name`** in semconv 1.43, which
> `otelgrpc` v0.70 adopted. The same change dropped `rpc.service` and made
> `rpc.method` the fully qualified `<service>/<method>`.
>
> The mediator reads both spellings (`RPCSystemNameKey`, then `rpc.system`), and
> names an `RPCMethod` entity `<rpc.service>/<rpc.method>` — falling back to
> `rpc.method` alone when `rpc.service` is absent. So the entity keeps the exact
> same name, e.g. `shop.v1.PaymentService/Authorize`, and this demo emits only
> the current convention rather than opting back in with
> `OTEL_SEMCONV_STABILITY_OPT_IN=rpc/dup`.
>
> The numeric `rpc.grpc.status_code` was replaced by the string
> `rpc.response.status_code` (`OK`, `INTERNAL`, `UNAVAILABLE`, ...). The mediator
> counts a request as failed when *either* the string matches `RCP_ERROR_CODES`
> or the number matches `GRCP_ERROR_CODES`, and both `INTERNAL` and `UNAVAILABLE`
> — the codes this demo's scenarios use — are in the string list.
>
> **The migration is fully transparent** — no entity is renamed and none is
> recreated. `getRPCServiceAndMethod` splits a fully qualified `rpc.method` back
> into service and method whenever `rpc.service` is absent, so the mediator
> reconstructs the same two values it used to read directly. Verified on a live
> cluster: all eleven `RPCMethod` entities kept both their names *and* their
> entity ids across the upgrade, so metric and SLO history is continuous.
>
> (An earlier version of this note claimed the ids change and history restarts.
> That was wrong — it reasoned from `RPCMethodId`'s inputs without accounting for
> the split.)
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
| gRPC client/server | `otelgrpc` v0.70 (`rpc.system.name`, fully qualified `rpc.method`, `rpc.response.status_code`) |
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

## External services

A CLIENT span to a hostname that nothing in the cluster claims (no pod, no Kubernetes Service,
no known NetworkEndpoint) becomes a plain `Service` entity named after the hostname, labelled
`causely.ai/service-type=External`. Its `Malfunction` and `Congested` root causes are what let
Causely say "the third party is the problem, not your services". The evidence for them is the
callers' own CLIENT spans:

- HTTP `http.response.status_code` ≥ 500 counts as an error; 401/403 and 429 count as
  unauthorized and throttled; other 4xx do not count
- the error-rate symptom on the caller's access needs a ratio above 4% at more than 0.3 rps

The demo's third parties are in-cluster stand-ins, so three rules keep them external. Break any
one of them and Causely ties the public hostname back to the stand-in pod, which then looks like
one of *your* services failing:

1. **Callers name the public API, not the stand-in.** `httpx.WithDialTo` puts
   `https://api.paypal.com/...` on the span (`url.full`, `server.address`, `server.port: 443`) and
   delivers the request to the in-cluster Service underneath otelhttp.
2. **No span is ever parented to that CLIENT span.** If the receiver emits a SERVER span whose
   parent is the caller's CLIENT span, the mediator bridges the hostname to the receiving
   Service. So the stand-ins run plain `net/http` without otelhttp, and `WithDialTo` strips
   `traceparent`, `tracestate` and `baggage` on the way out. Real third parties never get your
   trace context either.
3. **Beyla never sees the stand-in, from either side.** The Causely agent's Beyla instruments any
   process with an open port in `80,443,2000-10000`, but it instruments **by executable**: once
   one process matches, every process running that same binary inode is hooked too. Every shop
   service runs `/shopd`, so Beyla is inside all of them, the callers included. Two consequences:
   - **Caller side.** Beyla reads the request URL inside `net/http`, below otelhttp. So
     `WithDialTo` must not rewrite the URL: it redirects at **dial time**, and the request keeps
     `api.paypal.com` all the way down. The first version rewrote the URL. Beyla then reported
     `payment-gw → tracey-shop-stripe-sim` with the 503s, and Causely diagnosed the stand-in as
     well as the provider.
   - **Server side.** The stand-ins run `/partner-sim`, a separate COPY of the binary with its own
     inode (see the `Dockerfile`), and only listen on ports 18085–18090. Beyla therefore never
     selects them, and records no SERVER spans or errors against the stand-in pod.

The stand-ins also run their fault store `Quiet`. An ERROR log from a pod in your cluster is
evidence Causely weighs against that pod, and a real provider's logs are not yours to read. The
callers log instead (`payment processor unavailable, authorization not attempted`,
`provider_host=api.paypal.com`), which is what exonerates them.

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
| `get_entities(namespace_names=["tracey-shop"])` | every shop service present as a Service entity (the three sims have no spans, so they appear only as Kubernetes workloads), plus `api.paypal.com`, `api.easypost.com` and `api.sendgrid.com` as External services |
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
