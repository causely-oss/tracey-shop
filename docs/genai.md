# The genAI shopping assistant

Gives the demo a genAI surface, so Causely's AI entities have something real to model.

```
web-client / browser
  └─ storefront-bff   POST /api/assist        (SERVER)
     └─ ai-assistant  POST /assist            (SERVER)   ← the source Operation
        └─ "chat <model>"                     (CLIENT)   ← the genAI span
           server.address = model-gateway | api.openai.com | …
```

Everything ships **on**: `loadgen.assistRPS` is `0.5`, so the `AIModel` and `AIModelAccess`
entities and their token metrics are in Causely from the first install with no extra step. It costs
nothing, because the bundled `model-gateway` answers it.

```bash
./scripts/genai.sh         # show the current rate
./scripts/genai.sh 2       # speed it up, e.g. to make a scenario fire sooner
./scripts/genai.sh 0       # off
```

> **Pointing `genai.external` at a real provider makes that default a real bill** — roughly 1,800
> inferences an hour, unattended. Lower `loadgen.assistRPS` or set it to 0 when you do.

There is also an "Ask about this product" box on each product page in the browser storefront
(`make port-forward`), which shows the answer along with the model and token counts — the same
numbers that ride on the span.

## What Causely builds from it

| Entity | Shape |
|---|---|
| `AIModel` | Named **`<model>/<operation>`** — e.g. `mock-small-1/chat`. An `Asset`, **layered over the Service that `server.address` resolves to.** |
| `AIModelAccess` | `Source` → the calling Operation (`ai-assistant`'s `/assist`), `Destination` → the `AIModel`. |

Three symptoms on each, and two root causes:

| Symptom | Display name | Fires when |
|---|---|---|
| `InferenceDuration_High` | Inference Latency | p95 over threshold |
| `InferenceErrorRate_High` | Inference Error Rate | > 10% of inferences return HTTP ≥ 500 |
| `RateLimitedRate_High` | Rate Limited | > 5% of inferences return HTTP 429 |

**All three are gated on `@activation(condition=InferenceTotalRate > 0.1)`,** with an activation
delay of 10 over a 5-minute window. Below ~0.1 rps of genAI traffic the entities and token metrics
still appear and look healthy, but **no genAI symptom can ever fire** — which would make
`ai-model-malfunction` silently produce nothing. `loadgen.assistRPS: 0.5` leaves 5× headroom, and
`cmd/shopd/main_test.go` asserts every shipped values file stays above the gate. Failed inferences
still count toward `InferenceTotalRate`, so a high error rate never starves the condition.

- **`AIModel Congested`** ← Starvation ← `InferenceDuration_High`
- **`AIModel Malfunction`** ← Failure ← `InferenceErrorRate_High`

`AIModelAccess` propagates Failure and Starvation to its source Operation with probability 0.9,
which is how LLM trouble climbs the Operation → Service → SLO chain and reaches `storefront-bff`.

Token and inference *rates* (`TokenInputRate`, `TokenOutputRate`, `InferenceTotalRate`) are
recorded but have `symptom_enabled=false` — they are informational, so no amount of token spend
will ever raise a symptom on its own.

> **There is no hallucination, quality, eval, cost, tool-error or context-window detector.** Those
> three symptoms above are the entire genAI vocabulary, so a scenario can only ever target latency,
> errors or rate limiting.

## The ingest contract

**Every requirement below fails silently.** The mediator either never reaches its genAI analysis or
returns early, logging at Debug at most. There is no error, no warning, and no partial entity — the
inference simply never appears. That is why `scripts/verify-traces.sh` asserts each one, and why
`internal/genai/genai_test.go` exists.

Derived from the `causely` repo: `pkg/spananalyzer/semconv.go`,
`pkg/spananalyzer/span_analyzer.go` (`analyzeClientSpan`, `analyzeGenAISpan`),
`cmd/mediator/otlp/trace_client_gen_ai.go`, `pkg/model/operation_genai.go` and
`pkg/model/resolver.go`. Causely is on semconv **v1.39.0**; this repo pins **v1.34.0**, which
already carries every `gen_ai` constant needed.

| # | Requirement | Why |
|---|---|---|
| 1 | Span kind **CLIENT** | genAI is only analysed in the client pass. INTERNAL spans are ignored — and dropped at our own collector (`otelCollector.filterInternalSpans`). |
| 2 | **`gen_ai.system`** (or `gen_ai.provider.name`, or `llm.provider`) | Its **presence** is the *only* trigger that classifies a span as genAI, checked before HTTP, RPC and database. The value is never validated and is not even stored. Missing it makes the inference an ordinary HTTP call. |
| 3 | **`server.address`** (or `peer.service`) | Mandatory. `server.port` defaults to 443. |
| 4 | **`gen_ai.request.model`** non-empty (else `gen_ai.response.model`) | An empty model name is rejected outright. |
| 5 | `gen_ai.operation.name` | Taken verbatim into the entity name; never switched on. Defaults to `chat`. |
| 6 | `gen_ai.usage.input_tokens` / `output_tokens` as **integers** | The attribute reader accepts `IntValue` only. A float or string is read as **0**. |
| 7 | **`http.response.status_code`** | The *only* source of error and rate-limit signal: ≥500 → `InferenceError`, ==429 → `RateLimited`. **The OTLP span status is ignored**, so the `span.SetStatus` that drives every other error rate in this demo buys nothing here. Without this attribute a failed inference counts as a success. |
| 8 | A direct **SERVER-span parent** in the same service (or be the trace root) | Supplies the `AIModelAccess` source Operation. A trace root gets a synthesized `Background` operation instead. |

Requirement 8 is the subtle one, and it interacts with our collector config. An INTERNAL span
between the SERVER span and the genAI CLIENT span gets dropped by `filterInternalSpans`, which
orphans the parent chain: the `AIModel` still appears in the topology, but with **no access edge and
no metrics at all**. Causely's own `rag-genai.json` fixture is exactly this case — its genAI spans
sit under unregistered traceloop INTERNAL wrappers. So the provider call must happen directly in
`ai-assistant`'s handler.

Two further consequences worth knowing:

- **Exactly one CLIENT span per inference.** `internal/genai` creates the span itself and asks
  `httpx` for `WithCallerSpan()`, which skips the `otelhttp` transport. With both, there would be
  two CLIENT spans to the same peer, and Causely would build a spurious `HTTPPath` entity
  (`/v1/chat`) on the provider Service from the second.
- **Attributes outside the contract are discarded on ingest.** `gen_ai.request.temperature`,
  `gen_ai.response.finish_reasons`, `gen_ai.response.id` and the rest are dropped at parse time.
  They are still emitted, because they are correct OTel and useful to any other backend.

## Providers

`genai.api` selects the **wire protocol, not the vendor**:

| `genai.api` | Endpoint | Auth | Works with |
|---|---|---|---|
| `openai` | `POST {base}/v1/chat/completions` | `Authorization: Bearer` | OpenAI, Groq, Together, OpenRouter, Fireworks, DeepSeek, xAI, Mistral, Ollama, vLLM, LiteLLM, Azure OpenAI, Gemini's compat endpoint — and the bundled gateway |
| `anthropic` | `POST {base}/v1/messages` | `x-api-key` + `anthropic-version` | Anthropic |

Anything else needs a new shape in `internal/genai/genai.go`; the two above are ~40 lines each.

### The bundled gateway (the default)

`genai.external: ""` means the in-cluster **`model-gateway`** — role `llm-sim`, the same
identity-vs-role split the three partner simulators use. It speaks the real OpenAI wire format, so
the demo default exercises exactly the code path a real provider does rather than a mock branch that
could drift.

It is what keeps `helm install` free of prerequisites and the clean baseline achievable **with no
egress and no cost** — the same reasoning behind `partner-sim`. It is also why the deployed name is
`model-gateway` rather than `llm-sim`: Causely layers the `AIModel` over this Service, so this name
is what appears as the model's provider in the topology.

### A real provider

```bash
kubectl -n tracey-shop create secret generic tracey-shop-llm --from-literal=apiKey=sk-...

helm upgrade tracey-shop deploy/tracey-shop -n tracey-shop --reuse-values \
  --set genai.external=https://api.openai.com \
  --set genai.api=openai \
  --set genai.model=gpt-4o-mini \
  --set genai.apiKey.secretName=tracey-shop-llm
```

Setting `genai.external` skips the bundled gateway entirely. The key is injected into
`ai-assistant` alone, not through `commonEnv` into every pod — it is the one credential in this
chart that is not a committed demo default, so **never commit it**.

Causely handles an external hostname fine: with no workload owning `api.openai.com`, it synthesizes
a provider Service named after the endpoint and layers the `AIModel` over that. The in-cluster
default is still preferable for a demo, because it puts the model over a **real workload you can
inject faults into**.

Requires cluster egress. And note that the model name is half the `AIModel` entity name, so
switching providers creates a *new* entity rather than relabelling the old one.

## Cost

At `0.5` rps with the default 256-token cap and the short fixed prompt, expect roughly 1,800
inferences an hour at a few hundred input tokens each. On a cheap model that is cents per hour; on
a frontier model it is not. Two things bound it:

- `loadgen.assistRPS` is **absolute** and independent of `loadgen.rps`, so `./scripts/load.sh 100`
  cannot multiply genAI spend.
- `genai.maxTokens` caps each response, and `internal/services/aiassistant` truncates the question
  at 400 characters.

**The shipped default is 0.5 rps.** Do not drop below ~0.1: every genAI symptom is gated on
`InferenceTotalRate > 0.1`, so a slower trickle still produces the entities and token metrics but no
symptom can ever fire. `scripts/genai.sh` warns when you set a rate that low.

## Verifying

```bash
./scripts/genai.sh 0.5
make verify           # section 8 asserts the whole contract above
```

Then in Causely:

- `get_entities` for type `AIModel` in `tracey-shop` → expect `mock-small-1/chat`, provided by the
  `model-gateway` Service.
- `get_topology` → expect `ai-assistant` → `AIModelAccess` → `AIModel`. **An `AIModel` with no
  access edge means requirement 8 broke.**
- `get_metrics` for `inference_rate`, `inference_duration_p95`, `token_input_rate`,
  `token_output_rate`. Non-zero token rates prove requirement 6 end to end.
- `get_symptoms` → must be **empty**. The AI entities should exist and be healthy; nothing here is a
  fault scenario yet.

## Breaking it

`model-gateway` is an ordinary role behind the fault gate, so faulting the *provider* needs no new
code. One scenario ships:

```bash
./scripts/scenario.sh start ai-model-malfunction   # model-gateway errorRate=0.5
./scripts/scenario.sh stop  ai-model-malfunction
```

Expect **`AIModel Malfunction`** on `mock-small-1/chat` in 10-15 minutes; see
[scenarios.md](scenarios.md#ai-model-malfunction--the-failure-is-in-the-model-not-the-code) for the
full expectations, and `./scripts/genai.sh 2` to make it fire sooner.

The latency half has no scenario yet, but is the same one-liner:

```bash
kubectl -n tracey-shop port-forward deployment/tracey-shop-model-gateway 18090:8090 &
# +3s per inference -> InferenceDuration_High -> "AIModel Congested"
curl -X POST localhost:18090/admin/faults -d '{"latencyMs":3000}'
curl -X DELETE localhost:18090/admin/faults
```

The other seam is on the caller's side: `ai-assistant` passes its fault store to the provider
client, so `{"dependencyTimeoutMs":500}` on **`ai-assistant`** cuts the inference off below the
provider's real latency. That surfaces as a transport failure, which the client reports as HTTP 500
— so it lands on `InferenceError` too, but attributed to a slow *provider* seen from a client with
too tight a deadline. It is the genAI analogue of the `cart-timeouts` scenario.

Mind the thresholds and the activation delays: the error rate must exceed 10% and the rate must stay
above `InferenceTotalRate > 0.1`, and every genAI symptom has an activation delay of 10 — so allow
several minutes.

Both `model-gateway` and `ai-assistant` have **narratives** in `internal/faults/narrative.go`
(`inference request failed: model backend returned no completion` and
`chat completion failed, no answer returned to the shopper`), which is what lets Causely name the
failure mode instead of falling back to "inspect the application logs". Any new genAI scenario needs
one too — see the rules in [scenarios.md](scenarios.md#log-evidence).
