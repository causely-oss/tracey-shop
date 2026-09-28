# Topology

## Services

| Service | Layer | Inbound | Outbound |
|---|---|---|---|
| `browser` (a real user) | 0 | — | HTTP → storefront-bff |
| `web-client` | 0 | — | HTTP → storefront-bff |
| `storefront-bff` | 1 | HTTP :8080 | gRPC → catalog-api, checkout-api; HTTP → cart-service, ai-assistant (only when `genai.enabled`). Also serves the embedded browser storefront at `/` — untraced |
| `catalog-api` | 2 | gRPC :9001 | gRPC → inventory-svc; Valkey |
| `cart-service` | 2 | HTTP :8081 | Valkey |
| `checkout-api` | 2 | gRPC :9002 | HTTP → cart-service, shipping-quote; gRPC → pricing-engine, inventory-svc, payment-gw; Postgres; Kafka → `orders` |
| `inventory-svc` | 3 | gRPC :9003 | Postgres |
| `pricing-engine` | 3 | gRPC :9004 | Postgres; Valkey |
| `payment-gw` | 3 | gRPC :9005 | HTTP → api.paypal.com (stripe-sim); gRPC → ledger-svc |
| `shipping-quote` | 3 | HTTP :8082 | HTTP → api.easypost.com (carrier-sim) |
| `ledger-svc` | 4 | gRPC :9006 | Postgres; Kafka → `ledger.events` |
| `fraud-detector` | 4 | Kafka ← `orders` | gRPC → risk-model; Kafka → `notifications` |
| `risk-model` | 5 | gRPC :9007 | Valkey |
| `notification-worker` | 5 | Kafka ← `notifications` | HTTP → api.sendgrid.com (email-sim) |
| `ai-assistant` | 2 | HTTP :8088 | HTTP → model-gateway (or an external LLM provider) — emits the `gen_ai.*` CLIENT span |
| `model-gateway` | leaf | HTTP :8089 | — (the bundled OpenAI-compatible provider; role `llm-sim`) |
| `stripe-sim` | external | HTTP :18086 | — (answers for `api.paypal.com`; emits no spans) |
| `carrier-sim` | external | HTTP :18085 | — (answers for `api.easypost.com`; emits no spans) |
| `email-sim` | external | HTTP :18087 | — (answers for `api.sendgrid.com`; emits no spans) |

`fraud-detector` and `notification-worker` have no inbound port, so the chart renders no Service
for them. Their health cannot be inferred from a caller's error rate, which is what makes the
`fraud-lag` scenario a genuinely different failure shape.

The three sims are **third parties as far as Causely can tell**. Callers address the public API
(`https://api.paypal.com/v2/payments/authorizations` and so on), and the transport delivers to
the in-cluster sim underneath. The sims emit no spans and no logs, and listen above Beyla's port
range. So Causely shows `api.paypal.com`, `api.easypost.com` and `api.sendgrid.com` as
**External** services, and a fault on a sim reads as the provider failing — see the
`payment-provider-outage` and `email-provider-errors` scenarios. The in-cluster `*-sim`
Deployments still appear as Kubernetes workloads, but with no trace edges. Set a sim's
`publicURL` to `""` to call it under its own name instead, as an ordinary internal service.

## Protocol coverage

| Protocol | Where |
|---|---|
| HTTP | edge, cart-service, shipping-quote, all three third-party APIs, ai-assistant, model-gateway, web-client |
| gRPC | catalog, checkout, pricing, inventory, payment, ledger, risk (7 services) |
| Postgres | inventory-svc, pricing-engine, checkout-api, ledger-svc |
| Valkey (Redis) | catalog-api cache, cart-service store, pricing-engine rule cache, risk-model feature store |
| Kafka | 3 topics: `orders`, `ledger.events`, `notifications`; 2 consumer groups |

## Request flows

**Browse** (`GET /api/products`) — 3 hops plus cache/DB:

```
web-client → storefront-bff → catalog-api → [Valkey hit, or inventory-svc → Postgres]
```

**Search** (`GET /api/search`) deliberately bypasses the cache, so a steady stream of read
traffic always reaches Postgres and the database dependency stays visible in the topology even
when the cache is warm.

**Checkout** (`POST /api/checkout`) — the full fan-out. `checkout-api` orchestrates:

1. `GET /carts/{id}` → cart-service (HTTP) → Valkey
2. `CheckStock` → inventory-svc (gRPC) → Postgres
3. `Quote` → pricing-engine (gRPC) → Postgres + Valkey
4. `POST /quotes` → shipping-quote (HTTP) → api.easypost.com (HTTP)
5. `Authorize` → payment-gw (gRPC) → api.paypal.com (HTTP) **and** ledger-svc (gRPC) → Postgres + Kafka
6. `ReserveStock` → inventory-svc (gRPC) → Postgres
7. `INSERT` order + items → Postgres
8. `PRODUCE` → Kafka `orders`

Then, asynchronously, linked to the same trace by W3C context in the Kafka headers:

```
fraud-detector consumes orders
  → risk-model (gRPC) → Valkey feature store
  → PRODUCE notifications
      → notification-worker consumes
          → api.sendgrid.com (HTTP)
```

**Wholesale checkout** — the same `POST /api/checkout` fan-out as above, from a
B2B customer ordering by the case: four distinct products at case quantities
rather than a couple of units. It walks an identical path through the graph; only
the size of the order differs. Paced separately from the consumer mix and idle by
default (`loadgen.wholesaleRPS`, `scripts/wholesale.sh`).

**Assist** (`POST /api/assist`) — the genAI path, off by default:

```
web-client / browser → storefront-bff → ai-assistant → "chat <model>" (CLIENT, gen_ai.*)
                                                          → model-gateway, or a real provider
```

The inference span is a **direct child** of `ai-assistant`'s SERVER span, which is what supplies
the `AIModelAccess` source Operation on Causely's side. An INTERNAL span in between would be
dropped by the collector and would orphan the parent chain, leaving the `AIModel` with no access
edge at all. See [genai.md](genai.md) for the full ingest contract.

## Database schema

Applied idempotently at startup by every Postgres-using service, serialised with a
`pg_advisory_lock` so no migration Job or pod ordering is needed.

| Table | Owner | Purpose |
|---|---|---|
| `products` | inventory-svc | 200 deterministically seeded catalogue items |
| `inventory` | inventory-svc | available / reserved counts |
| `price_rules` | pricing-engine | 24 discount rules across 8 categories × 3 tiers |
| `orders`, `order_items` | checkout-api | placed orders |
| `stock_reservations` | inventory-svc | reservation records |
| `ledger_entries` | ledger-svc | balanced double-entry pairs |

Seed data is deterministic on purpose: the load generator picks product ids by index
(`P0001`–`P0200`), so every replica and every restart agrees on what exists and the clean
baseline never produces a 404.

## Ports

| Port | Purpose |
|---|---|
| 8080–8089 | HTTP business ports (Service port; the container listens on +10000, e.g. 18080) |
| 9001–9007 | gRPC business ports (Service port; the container listens on +10000, e.g. 19005) |
| 18085–18087 | the third-party stand-ins |
| **18090** | admin: `/healthz`, `/readyz`, `/admin/faults` — **never trace-instrumented** |
| 4317 / 4318 | collector OTLP receiver |

Three deliberate choices:

- No process listens in **2000–10000**. The Causely agent's Beyla instruments processes by
  listening port in that range, and would report every call a second time alongside the SDK.
  For the third-party APIs that second copy overwrites ours and hides the provider's errors from
  Causely (see [causely-setup.md](causely-setup.md#external-services)). The Services keep the
  familiar ports; only the containers listen +10000.

- Business ports avoid **4317**. Causely drops any dependency whose destination port is 4317, so
  the app→collector traffic never becomes a spurious topology edge — but a business service
  listening there would be dropped too.
- Health endpoints live only on the admin port. Causely filters health-ish paths
  (`/health`, `/ready`, `/metrics`, …) anyway, but keeping probes off the traced listener means
  kubelet traffic never enters a trace at all.
