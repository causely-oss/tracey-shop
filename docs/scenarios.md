# Fault scenarios

All faults default to off. `helm install` produces a clean baseline; a scenario is staged at
runtime by POSTing to the affected pods' admin ports:

```bash
./scripts/scenario.sh start <name>
./scripts/scenario.sh stop  <name>
./scripts/scenario.sh stop-all
./scripts/scenario.sh status
```

`stop` clears every fault on the affected services **and** releases the memory and connections
that the leak faults accumulated, so a service recovers without a restart.

Faults are per-process state, held in each pod's own fault store. `scenario.sh` therefore applies
them to **every replica** of the affected service and prints a `✓ <service>: applied to all N
replica(s)` line. If it reports fewer than the full count, stop and retry — a fault on one of two
pods is halved by the Service's load balancing, and the demo would show a weaker signal than the
scenario describes.

## Match the load mix to the scenario

This catches people out. The default mix is only **5% checkout**, so a fault confined to the
checkout path is heavily diluted by the time it reaches the storefront:

```
35% failure at payment-gw  ×  5% of traffic being checkouts  ≈  1.75% error rate at storefront-bff
```

Verified on a live cluster: with `payment-errors` active, direct probes of `POST /api/checkout`
failed 33% of the time — exactly as injected — while the storefront's overall error rate sat under
2%. That may be too thin for an error-rate symptom to fire at all.

If a **human** is clicking through the browser storefront rather than the load generator, the fix is
different: use `payment-outage` (100%) instead of `payment-errors` (35%), so the failure lands on the
first click. See that scenario below.

For any scenario on the checkout path (`payment-errors`, `ledger-slow-queries`, `cart-timeouts`,
`ledger-pool-exhaustion`), weight the mix toward checkout first:

```bash
helm upgrade tracey-shop deploy/tracey-shop -n tracey-shop --reuse-values \
  --set loadgen.mix.checkout=40 \
  --set loadgen.mix.browse=30 \
  --set loadgen.mix.search=10 \
  --set loadgen.mix.viewProduct=10 \
  --set loadgen.mix.addToCart=10
```

That takes a loadgen restart (the mix is read at startup, unlike `rps`). It is worth doing before
the demo rather than during it.

Scenarios on the browse path (`catalog-cache-miss`, `pricing-cpu`) need no change — the default mix
is already 85% browse/search/view.

## Log evidence

Causely synthesises its root-cause *description* from WARN/ERROR container logs, not only from
metric symptoms. Without a log line, a scenario yields a generic description ("inspect the
application logs..."); with one, Causely can name the actual failure mode.

Every scenario therefore emits a matching log. Two rules govern them, both enforced by tests in
`internal/faults/narrative_test.go`:

1. **Nothing may reveal the injection.** No message mentions faults, injection, scenarios or the
   demo. This is not theoretical — an earlier version logged `fault spec updated` at WARN, and
   Causely reported the root cause as *"Fault spec updated causing payment authorization
   malfunction"* with remediation *"revert the fault specification update"*. Accurate, but it tells
   the audience the incident was staged.
2. **Each message is specific to its service** — the failing operation, the dependency, and the
   numbers an engineer would want.

| Scenario | Level | Message | Structured fields |
|---|---|---|---|
| `payment-errors` / `payment-outage` | ERROR | payment authorization rejected by acquirer: no auth code returned | `observed_failure_rate`, `retryable` |
| `ledger-slow-queries` | WARN | double-entry journal write exceeded its query budget | `statement`, `duration_ms`, `threshold_ms`, `db_system` |
| `inventory-oom` | WARN | stock projection cache is growing without bound | `retained_entries`, `retained_mb`, `bytes_per_entry` |
| `fraud-lag` | ERROR | order event processing halted, offsets are no longer committing | `topic`, `consumer_group`, `partition`, `stalled_at_offset` |
| `pricing-cpu` | WARN | price rule evaluation exceeded its CPU budget | `cpu_ms`, `budget_ms` |
| `cart-timeouts` | WARN + ERROR | cart read latency degraded against the session store **and** downstream call exceeded deadline, abandoning order placement | `dependency`, `deadline_ms` |
| `catalog-cache-miss` | WARN | product cache unavailable, serving reads from inventory-svc | `cache_key`, `hit_ratio`, `db_system` |
| `ledger-pool-exhaustion` | ERROR | ledger connection pool exhausted, journal writes are queueing | `connections_in_use`, `pool_size` |
| `risk-crash` | ERROR + panic | unrecoverable error scoring order: feature vector dimension mismatch | (also the panic message, so the stack trace reads plausibly) |
| `checkout-latency` | WARN | checkout orchestration latency degraded | `duration_ms`, `threshold_ms` |

`cart-timeouts` is the one worth understanding. It emits on **both** sides: a latency warning at
`cart-service` (the real cause) and a deadline error at `checkout-api` (the visible victim) naming
`cart-service` as the dependency that blew the deadline. That second log is what should stop Causely
concluding checkout-api is the origin.

Messages are defined per service in `internal/faults/narrative.go`; add or reword them there.

### Log volume

These are rate-limited to **one line per message per 5 seconds per pod**, with a
`suppressed_similar` count on each emission so the true rate stays visible. Without that,
`pricing-cpu` — which fires on the browse path, ~85% of traffic — would emit tens of lines per
second per pod at 40 rps.

A clean baseline emits **nothing**: `Gate` returns before touching the logger when no fault is set.

## A depleted warehouse silently invalidates every checkout-path scenario

`ReserveStock` decrements inventory on every order, and originally nothing put it back. After ~49k
orders, **162 of 200 products sat at zero**, `CheckStock` failed for nearly every cart, and checkout
returned 500 on ~100% of attempts.

The damaging part is not "checkout is broken" — it is that checkout fails at the **stock check, step 2
of 8**, so it never reaches payment-gw. A `payment-errors` run then produced *no payment signal at
all*: Causely measured payment's error rate at ~2.4% instead of 35%, below its ~2% symptom threshold,
so there was no payment symptom and no payment root cause — just a root cause on checkout, which is
genuinely where the requests were dying.

`internal/store.StartRestocker` (run from inventory-svc) now tops any product below 200 units back to
1000 every 2 minutes, and resets `reserved`, which was also growing without bound (147k in a day).

If a checkout-path scenario ever produces no signal on the service you faulted, check this first:

```bash
kubectl -n tracey-shop exec statefulset/tracey-shop-postgres -- psql -U shop -d shop -tAc \
  "SELECT count(*) FROM inventory WHERE available = 0"
```

Anything other than 0 means checkout is failing before it reaches your fault. A quick manual top-up:

```sql
UPDATE inventory SET available = 1000, reserved = 0 WHERE available < 200;
```

More generally: **when a scenario produces no signal, verify the request actually reaches the faulted
service.** Probing `POST /api/checkout` directly and comparing the failure rate against the injected
rate takes a minute and is decisive — 100% failures when you injected 35% means something upstream is
failing first.

## Leave time between scenarios, or the wrong service will outrank the real cause

**Error budgets are a 1-day rolling window.** A service damaged by one scenario keeps a blown budget
for ~24 hours, and Causely's severity rules give any root cause on a service with a *violated* SLO the
Critical bucket:

```
≥2 active symptoms + a violated SLO  → 12.0  → Critical
≥2 active symptoms + an at-risk SLO  →  8.0  → High
```

So a service you broke earlier will sort **above** the service you are breaking now.

Observed exactly this: after a `fraud-lag` run left `checkout-api`'s `RequestSuccessRate` at −12.5%,
a later `payment-errors` run produced

| Root cause | Entity | Severity |
|---|---|---|
| Service Malfunction | `checkout-api` | **Critical** |
| Faulty Error Handling in RPC Method | `shop.v1.PaymentService/Authorize` | **High** |

Both are Urgent and the payment one is correct — but checkout appeared first and looked like the
epicentre, purely because of residual budget burn from the *previous* scenario.

Three ways to handle it:

1. **Leave ~24 h between destructive scenarios** for a clean read. Best for a rehearsed demo.
2. **Install under a fresh release name** — `helm install tracey-shop-b -n tracey-shop-b` creates new
   Service entities with untouched SLOs. Fastest way to get a clean slate on demand.
3. **Accept it and say so.** Explaining that checkout still has a spent error budget from the earlier
   incident is a legitimate and quite realistic observability story.

Check before demoing:

```
get_slo(namespace_names=["tracey-shop"], only_violated=true)
```

Anything listed there will inflate a root cause on that service. Note the auto-created
`AvailabilityRate` SLO is also violated on **every** service after a busy day of `helm upgrade`s,
since each rollout is brief unavailability — that one is deployment churn, not your scenario.

## Timing

Causely's symptoms are thresholded over a window, so a freshly injected fault does not surface
instantly. Error-rate and latency symptoms typically take **10–15 minutes** at default load.
Two ways to shorten that:

```bash
./scripts/load.sh 100     # more traffic crosses the threshold sooner
```

or pick a scenario with an infrastructure signal (`inventory-oom`, `risk-crash`), which show up
as Kubernetes events within a minute or two.

Sequence for a demo:

1. Confirm the baseline is clean — `get_symptoms` for the namespace should be empty.
2. `./scripts/load.sh 100`
3. `./scripts/scenario.sh start <name>`
4. Wait, then check `get_diagnoses` / `get_issues`.
5. `./scripts/scenario.sh stop <name>` and confirm the diagnosis clears.

## The scenarios

### `payment-errors` — error rate three layers down

```
payment-gw  errorRate=0.35
```

35% of authorisations return gRPC `INTERNAL`. Errors propagate up through `checkout-api` to
`storefront-bff`, so **three services show an elevated error rate** but only one is the cause.
`payment-gw`'s own dependencies (`stripe-sim`, `ledger-svc`) stay healthy.

- **Expected root cause:** `payment-gw`
- **Expected symptoms:** `RequestErrorRate_High` on payment-gw, checkout-api, storefront-bff
- **The test:** does Causely name the deepest failing service rather than the first one to report?

### `payment-outage` — every authorisation fails

```
payment-gw  errorRate=1.0
```

Same fault as above, turned to 100%. **Use this whenever a human is clicking through the browser
storefront** rather than letting the load generator drive.

At 35% a presenter has a **65% chance of a successful checkout on any given click**, and runs of
six consecutive successes are entirely normal — measured on a live cluster. To an audience that
reads as "the demo isn't working". At 100% the failure lands on the first click, every time.

Browsing, search and product pages keep working, so the shop still looks alive and only the
checkout breaks — which is the story you want on screen.

| Clicks | Chance of seeing NO failure at 35% |
|---|---|
| 1 | 65% |
| 2 | 42% |
| 3 | 28% |
| 5 | 12% |

- **Expected root cause:** `payment-gw`, same as `payment-errors` but stronger and faster
- **Use `payment-errors` instead** when you want the realistic partial-failure story for Causely,
  or when the load generator rather than a human is producing the traffic

### `ledger-slow-queries` — latency at the bottom of the deepest chain

```
ledger-svc  slowQueryMs=800
```

800ms of `pg_sleep` inside each ledger write. The delay is *database* time, not application time,
so it shows up in Postgres' statistics and in Causely's slow-query view. Latency propagates five
hops up to the storefront. **No errors anywhere** unless timeouts start biting.

- **Expected root cause:** `ledger-svc`, attributed to its Postgres queries
- **Expected symptoms:** `RequestLatency_High` on ledger-svc, payment-gw, checkout-api, storefront-bff
- **The test:** a pure-latency chain with no error signal to follow.

### `inventory-oom` — memory leak to OOMKill

```
inventory-svc  memLeakKbPerReq=256
```

Retains 256KiB per request. Under default load the container reaches its memory limit in a few
minutes and is OOMKilled, then restarts and starts over.

- **Expected root cause:** `inventory-svc`, OOMKilled / restarting
- **Expected symptoms:** `MemoryUtilization_High`, then pod restarts and request failures on
  `catalog-api` and `checkout-api`
- **The test:** correlating a Kubernetes-level event with the application errors it caused.

### `fraud-lag` — consumer lag with no caller to complain

```
fraud-detector  consumerStall=true
```

The consumer stops committing offsets but stays healthy: liveness passes, no errors, no restarts.
Because nothing calls `fraud-detector` synchronously, **no upstream service reports anything**.
The only signal is growing lag on the `orders` topic, and downstream `notification-worker` going
quiet.

Because `checkout-api` applies **order-intake backpressure** (below), the backlog does not stay
harmless: once it passes 500 messages, checkout starts shedding orders, which drags its own SLOs and
`storefront-bff`'s down with it.

- **Expected symptoms:** `Consumer Lag` (internally `Lag_High`) on the `ConsumerTopicAccess` entity
  named `Background consumes orders` — *not* on the Topic — plus `RequestErrorRate_High` on
  `checkout-api` and `storefront-bff`, and `BurnRate_High` on checkout-api's success SLO
- **Expected root cause:** **`Slow Consumer`**, classified **Urgent**
- **The test:** the cause sits in the asynchronous branch with no synchronous caller, three hops from
  where the errors actually appear.

### Why this one needs backpressure to be Urgent

Causely classifies a root cause Urgent only when its severity reaches 8.0, and that requires **an
SLO actually at risk *and* at least two live symptoms**. With a single symptom the score is capped
just below 8.0 no matter how many SLOs are at risk. Consumer lag on its own carries the model's
static weight of 2.0 — Low, Non-Urgent, one symptom, no impact.

Causely's model already predicts the impact path for a Kafka topic (there is no Topic SLO, so it
routes through the producer):

```
Lag.High → Topic.Clogged → Producers.Clogged → Starvation → producer LatencySLO / SuccessSLO
```

`checkout-api` is the producer of `orders`, so **order-intake backpressure is what makes that
predicted chain actually happen**: it watches the `fraud-workers` lag and, past
`orderIntakeBackpressure.lagThreshold` (500), rejects a fraction of orders with gRPC
`UNAVAILABLE` and adds a queueing delay to the rest.

```yaml
orderIntakeBackpressure:
  enabled: true
  lagThreshold: 500    # ~4 minutes of stall at ~2 orders/sec
  rejectRate: 0.25     # 25% of checkouts shed
  latency: 300ms       # moves the latency SLO too, not just the success SLO
  pollInterval: 15s
```

> **The reject code matters.** It must be one of the gRPC codes otelgrpc maps to an **Error** span
> status — `Unknown`, `DeadlineExceeded`, `Unimplemented`, `Internal`, `Unavailable`, `DataLoss`.
> Causely decides whether a server span counts against a service's error rate from the span status
> alone, so anything else is invisible. `ResourceExhausted` is the intuitive choice for load shedding
> and maps to **Unset**: the first version shed 21% of orders, storefront returned 5xx and the load
> generator saw the failures, yet checkout-api's error rate in Causely stayed flat and the root cause
> stayed Non-Urgent. A test in `internal/services/checkout/checkout_test.go` enforces the code.

This is ordinary application behaviour, not a fault — a checkout service publishing to a review
queue it cannot outrun has to stop accepting orders eventually. At a clean baseline the backlog is
near zero, so it never engages and logs nothing. Measured on a live cluster: 0 failures out of 3600
requests at baseline; ~21% of checkouts shed once the stall pushed the backlog past 500, which is
1.06% of all storefront traffic — comfortably past storefront-bff's 1% error budget.

It emits its own evidence log, with a trace id attached:

```
ERROR shedding order intake: fraud review backlog is beyond capacity
      review_backlog=604 backlog_threshold=500 topic=orders consumer_group=fraud-workers
```

**Full timeline:** ~4 min for the backlog to cross 500, then ~5–10 min for the 15-minute burn-rate
window to push the SLO to at risk. Budget **~15 minutes** end to end. Left running, the SLO moves
from at risk (High) to violated (Critical).

> **This scenario needs a metric source, unlike every other one.** Traces cannot carry consumer lag.
> They create the Topic and `ConsumerTopicAccess` entities, and the `ConsumerGroup` /
> `ConsumesTopic` labels Causely matches on — but the `Lag` attribute itself only ever comes from a
> metric. The chart therefore deploys a `kafka-exporter`
> (`causelyIntegrations.kafka`) publishing `kafka_consumergroup_lag`, which is exactly what Causely's
> shipped Prometheus config queries. Verify with `make verify-lag`. Without it, real lag piles up in
> Kafka and Causely stays completely silent.

**Timing.** `Lag_High` fires when the **5-minute average** of `Lag` exceeds **50 messages**, and
Causely's Prometheus scraper syncs every 60s — so allow **~6 minutes** after starting the scenario.
Measured on a live cluster: lag reached ~1100 per partition within a minute, and the symptom
appeared about six minutes in.

**Why the stalled consumer still emits spans.** `consumerStall` receives each message, emits its
CONSUMER span (marked failed), and then simply never commits the offset — that missing commit is what
produces the lag. It deliberately does *not* block before creating the span.

This matters more than it looks. Causely attaches the `Lag` metric to the `ConsumerTopicAccess`
entity, and that entity exists **only** because of consumer spans; the metric can never create it.
An earlier version blocked before emitting any span, so while stalled the entity stopped being
refreshed. It worked at first only because the entity pre-dated the stall — but when the mediator
restarted and lost its graph, the entity vanished and the scenario went permanently silent while lag
climbed past 5000 messages. If you ever change `ConsumeClaim` in
`internal/transport/kafkax/kafkax.go`, keep the span on the stalled path.

### `pricing-cpu` — CPU throttling

```
pricing-engine  cpuBurnMs=40
```

40ms of busy-loop per request. At default load this exceeds the container's CPU limit and CFS
throttles it, which turns into latency rather than errors.

- **Expected root cause:** `pricing-engine`, CPU throttled
- **Expected symptoms:** `CPUThrottled_High` on pricing-engine, `RequestLatency_High` on
  checkout-api and storefront-bff
- **The test:** distinguishing a resource-limit cause from an application-logic one.

### `cart-timeouts` — the cause is not where the errors are

```
cart-service  latencyMs=1500
checkout-api  dependencyTimeoutMs=500
```

`cart-service` becomes slow but never fails. `checkout-api`'s outbound timeout is tightened below
that latency, so **checkout-api is where the errors appear** while cart-service's own error rate
stays at zero.

- **Expected root cause:** `cart-service` (latency), surfacing as errors on `checkout-api`
- **The test:** the most misleading shape in the set — the erroring service is the victim.

### `catalog-cache-miss` — a load shift with no failure at all

```
catalog-api  disableCache=true
```

Every catalogue read bypasses Valkey and goes through to `inventory-svc` and Postgres. Nothing
errors; nothing crashes. Postgres query volume jumps several-fold and latency rises across the
read path.

- **Expected observation:** load and latency shift onto `inventory-svc` and Postgres
- **The test:** a degradation with no error signal and no infrastructure event.

### `ledger-pool-exhaustion` — connection starvation

```
ledger-svc  dbConnLeak=true
```

Leaks one pool connection per request. Once the pool (12 connections) is exhausted, every
subsequent query blocks until its context deadline, and `ledger-svc` starts failing.

- **Expected root cause:** `ledger-svc`, connection-pool exhaustion
- **The test:** a self-inflicted resource exhaustion that looks like a database problem.

### `risk-crash` — CrashLoopBackOff on the async branch

```
risk-model  panicRate=0.02
```

2% of requests panic, killing the process. Kubernetes restarts it, repeatedly.

- **Expected root cause:** `risk-model`, CrashLoopBackOff
- **Expected symptoms:** pod restarts; `fraud-detector` errors as its gRPC calls fail
- **The test:** attributing an async-branch crash to its effect on the consumer above it.

### `checkout-latency` — control case

```
checkout-api  latencyMs=400  latencyJitterMs=200
```

Latency injected at the orchestrator itself. Cause and symptom are the same service.

- **Expected root cause:** `checkout-api`
- **The test:** a sanity check. If Causely gets this wrong, something is misconfigured.

## Fault reference

Every service reads the same fault spec, so any of these can be applied to any service:

| Field | Effect |
|---|---|
| `errorRate` | 0..1 — fraction of requests returning 500 / gRPC `INTERNAL` |
| `latencyMs`, `latencyJitterMs` | added handler latency, fixed plus jitter |
| `slowQueryMs` | `pg_sleep` before each DB call, so the delay is database time |
| `dbConnLeak` | leaks a pool connection per request |
| `memLeakKbPerReq` | retains N KiB per request |
| `cpuBurnMs` | busy-loops per request |
| `consumerStall` | Kafka consumer stops making progress |
| `dependencyTimeoutMs` | overrides outbound client timeouts |
| `panicRate` | 0..1 — fraction of requests that crash the process |
| `disableCache` | forces cache misses |

Apply an ad-hoc combination directly:

```bash
kubectl -n tracey-shop port-forward deployment/tracey-shop-payment-gw 18090:8090 &
curl -X POST localhost:18090/admin/faults \
  -H 'Content-Type: application/json' \
  -d '{"errorRate":0.1,"latencyMs":250}'

curl localhost:18090/admin/faults              # inspect
curl -X DELETE localhost:18090/admin/faults    # clear
```

Compose your own scenario by adding a case to `scenario_spec()` in `scripts/scenario.sh`.
