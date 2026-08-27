# Contributing

Thanks for looking. This is a demo application: its job is to produce **credible** failures that
Causely can diagnose, so most of the conventions below exist to protect that credibility.

## Development loop

```bash
make lint            # go vet + helm lint
make test            # unit tests, including chart/code drift checks
make kind-up         # a local cluster
make kind-load       # build the image and side-load it
make deploy          # install that local image
make redeploy        # rebuild, reload, upgrade, restart — the inner loop
```

`make test` is fast and catches most mistakes. Please run it before opening a PR; CI runs the same
thing plus `helm template` against all three values files.

### Regenerating the gRPC stubs

`gen/` is checked in on purpose, so building the demo needs no protoc. If you change
`proto/shop/v1/shop.proto`:

```bash
make proto-tools     # installs protoc-gen-go and protoc-gen-go-grpc at the pinned versions
make proto
```

`protoc` itself must already be on your PATH. The plugin versions in `proto-tools` are pinned to the
ones that generated the checked-in files — a different version produces unrelated diff noise.

## Adding a service

Two places:

1. A block under `services:` in `deploy/tracey-shop/values.yaml` — port, protocol, replicas,
   dependencies.
2. A package under `internal/services/<name>/`, registered in the `ROLE` dispatch in
   `cmd/shopd/main.go`.

Everything is one Go module and one image; `ROLE` selects which service the container runs, and Helm
renders each role as its own Deployment and Service. Causely still sees genuinely distinct services
with distinct `service.name` values.

Update [docs/topology.md](docs/topology.md) in the same PR.

## Adding a fault scenario

Four places, and the tests will tell you if you miss one:

1. The injector in `internal/faults/`.
2. A case in `scripts/scenario.sh` (both the `start` spec and the `list` description).
3. A row in the README table and a section in [docs/scenarios.md](docs/scenarios.md).
4. A **narrative** — the WARN/ERROR log line the scenario emits.

### The narrative rule, and why it is a test

Causely builds its root-cause *description* from container logs, not only from metric symptoms. A
scenario with no log line gets a generic description; a scenario whose log line mentions the
injection gets a description that gives the game away.

`internal/faults/narrative_test.go` enforces this. A narrative must not contain any of: `fault`,
`inject`, `injected`, `scenario`, `simulat`, `synthetic`, `demo`, `chaos`, `artificial`,
`deliberate`, `test harness`.

This came from a real regression: an earlier version logged `fault spec updated` at WARN, and
Causely duly reported the root cause as *"Fault spec updated causing payment authorization
malfunction"* with the remediation *"revert the fault specification update"*.

Write messages a real on-call engineer would recognise —
`ledger connection pool exhausted, journal writes are queueing` — and never mention the mechanism.

### The log-level rule

`internal/obs/logging_test.go` walks the source and fails if a control-plane event (config reload,
fault-spec change, admin activity) is logged above Info. Anything at WARN or ERROR is evidence
Causely will build a root cause out of, so control-plane chatter has to stay at Debug or Info.

Error strings have the same problem from the other direction: gRPC error text propagates up the
whole call chain into the JSON body `storefront-bff` returns to the browser, so it must never carry
internal detail. See `ErrInjected` in `internal/faults/faults.go`.

## No environment-specific values

This repo was extracted from one author's cluster, and CI has a `hygiene` job that fails if the
values that were hard-coded in it ever come back — mediator namespaces, registry account ids, node
instance types, cluster names.

The usual way it happens is pasting a working `--set` line out of your terminal into a doc. Use
placeholders (`mediator.<namespace>:4317`, `<account>.dkr.ecr.<region>.amazonaws.com`) instead.

The defaults must work against a **stock** Causely install: mediator in the `causely` namespace, the
published image, no edits. CI asserts that too.

## Known follow-ups

- Publish the Helm chart as an OCI artifact. Deliberately not done yet: every documented workflow
  (`scripts/scenario.sh`, `scripts/verify-traces.sh`, `make mediators`) needs a clone, so a chart
  without `scripts/` would be half a demo.
- `go test -race` in CI, once the current suite has been green for a while — the `grpcx` tests spin
  real listeners with dial retries.

## License

By contributing you agree that your contributions are licensed under [Apache 2.0](LICENSE).
