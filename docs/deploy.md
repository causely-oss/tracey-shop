# Deploying to a Kubernetes cluster

The demo is the same everywhere. On a real cluster only two things differ from kind: the image is
pulled from a registry rather than side-loaded, and the mediator endpoint is a real one.

The image is published, so there is **no build step** in the normal path.

## The one thing that will bite you first

The chart defaults to **`mediator.causely:4317`**, which is correct for a standard Causely install.

**On an install where the Causely control plane and the mediators are split, the mediator is not in
the `causely` namespace.** That namespace then holds only the backend — `analysis`, `api`,
`background`, `gateway`, `ui` — and contains no `mediator` Service at all. Each monitored
environment gets its own mediator in its own namespace, so you will see one
`mediator.<environment>:4317` per environment and the default will not resolve.

This fails in the worst way available: the application pods are healthy, the collector accepts
spans, nothing restarts, and only the collector's own exporter logs show the failure. The demo
simply never appears in Causely.

So discover it rather than assuming:

```bash
make mediators      # lists every mediator.<namespace>:4317 in the current cluster
```

The protocol is **OTLP gRPC on 4317**, plaintext. Not HTTP, and not `54318` — that port is the
mediator's own self-telemetry receiver.

## Steps

### 1. Point kubectl at the cluster

```bash
kubectl config current-context   # confirm before going further
```

See [Cloud provider notes](#appendix-cloud-provider-notes) if you need the command that sets this.

### 2. Deploy

```bash
make mediators
make deploy-cloud MEDIATOR=mediator.causely:4317
```

That prints the context, image and mediator it is about to use, then installs into the
`tracey-shop` namespace using `values.yaml` + `values-cloud.yaml`. Preview it first with
`make template-cloud MEDIATOR=...`.

If your cluster has no Causely install and you just want to watch the app run, add
`--set causelyIntegrations.postgres.enabled=false` — see
[Running without Causely](#running-without-causely).

### 3. Verify

```bash
make status                              # every pod Running, zero restarts
./scripts/verify-traces.sh --upgrade     # assert the attributes Causely needs
make collector-logs                      # confirm no exporter errors
```

Then from the Causely side, confirm the topology built:

- `get_entities(namespace_names=["tracey-shop"])` → 15 Service entities
- `get_topology` → five layers, plus Postgres/Valkey database entities and Produces/Consumes edges
  on `orders`, `ledger.events` and `notifications`
- `get_symptoms` for the namespace → **empty**, because nothing is broken yet

### 4. Run a scenario

```bash
./scripts/load.sh 100
./scripts/scenario.sh start payment-errors
# wait 10-15 min for the error-rate symptom to fire
./scripts/scenario.sh stop payment-errors
```

### Tear down

```bash
make undeploy NAMESPACE=tracey-shop
```

If you enabled persistence, that leaves PVCs behind — `kubectl -n tracey-shop get pvc` and delete
them explicitly.

## Building and publishing your own image

Only needed if you have changed the code, or if your cluster cannot reach `ghcr.io`.

```bash
docker login <registry>
make push REGISTRY=<registry> IMAGE_NAME=<org>/tracey-shop TAG=v0.1.0
make deploy-cloud IMAGE_REPO=<registry>/<org>/tracey-shop DEPLOY_TAG=v0.1.0 MEDIATOR=...
```

> **Build multi-arch, and keep it that way.** `make push` defaults to
> `linux/amd64,linux/arm64`. A node group's architecture can change without warning, and every pull
> of a single-arch image then fails with `no match for platform in manifest` — leaving pods in
> `ImagePullBackOff` and the Helm release in a `failed` state, with nothing in the application logs
> to explain it. Building both costs seconds, because the Dockerfile cross-compiles with Go
> (`--platform=$BUILDPLATFORM` + `GOARCH=$TARGETARCH`) rather than emulating the target under QEMU.
> Override with `make push PLATFORMS=linux/arm64` if you really want one arch.

> **Use a fresh tag for every code change.** `image.pullPolicy` is `IfNotPresent`, so pushing a new
> digest to the *same* tag does **not** re-pull — the nodes keep the cached image and a
> `helm upgrade` quietly runs your old binary, even though `docker push` reported success. Deploy a
> unique tag:
>
> ```bash
> TAG="0.1.1-$(date +%Y%m%d%H%M)"
> make push deploy-cloud TAG="$TAG" DEPLOY_TAG="$TAG" MEDIATOR=mediator.causely:4317
> ```
>
> A redeploy replaces the pods, which **resets every injected fault** — re-run
> `./scripts/scenario.sh start <name>` afterwards.

## What `values-cloud.yaml` changes, and why

| Setting | Value | Reason |
|---|---|---|
| `loadgen.rps` | 40 | a shared cluster absorbs more than a laptop, and more traffic makes symptoms fire sooner |
| backend resources | raised | room to breathe on multi-vCPU nodes |
| collector resources | raised | more spans per second to batch |
| persistence | **off** | see below |

Everything else — the topology, replica counts, per-service limits, the image and the mediator
endpoint — comes from the base `values.yaml`, so every target runs the same demo.

### Why persistence is off by default

The schema and seed data are applied idempotently at startup, so Postgres re-seeds itself after a
restart and the only thing lost is order history, which no scenario depends on. Leaving PVCs out
keeps `helm uninstall` clean, which matters on a cluster you share.

To make it durable, uncomment the `persistence` blocks in `values-cloud.yaml` and set a
`storageClass` your cluster actually has (`kubectl get storageclass`). Check
`allowVolumeExpansion` on it first — several common defaults are `false`, and you cannot grow the
volume later, so size it correctly up front.

## Cluster requirements

| Requirement | What to check |
|---|---|
| Node architecture | amd64 or arm64 — both are published, so either works |
| CPU | ~2 vCPU across the whole demo; rarely the constraint |
| **Memory** | **usually the binding constraint.** ~3GB requested, ~8GB of limits across ~20 pods. Run `kubectl top nodes` first |
| StorageClass | only needed if you enable persistence. Check `allowVolumeExpansion` |
| Pod Security admission | the image is distroless and runs as nonroot, so `restricted` is fine |
| NetworkPolicies | egress from the release namespace to the mediator's namespace must be permitted. A default-deny policy will silently stop ingest |
| ResourceQuota / LimitRange | if the namespace has one, compare it against the requests above |

If pods go `Pending`, prefer scaling replicas down over cutting CPU — or deploy with
`values-kind.yaml`, which is single-replica throughout but keeps the identical topology:

```bash
helm upgrade --install tracey-shop deploy/tracey-shop -n tracey-shop --create-namespace \
  -f deploy/tracey-shop/values-kind.yaml \
  --set otelCollector.exporter.endpoint=mediator.causely:4317
```

Multiple demos can report to the same mediator and will appear in Causely as separate namespaces.
To keep one demo's scenarios from being confused with another's, point them at different
environments.

## Running without Causely

The PostgreSQL integration creates a Secret, a Role and a RoleBinding in the **mediator's**
namespace, because Causely's scraper reads secrets from its own namespace. On a cluster with no
Causely installed, that namespace does not exist and `helm install` fails with:

```
Error: ... namespaces "causely" not found
```

That is the integration, not the app. Turn it off:

```bash
--set causelyIntegrations.postgres.enabled=false
```

Everything except the native PostgreSQL scraper still works — all 15 services, all 11 scenarios,
the browser storefront, and the trace pipeline.

## Using a collector you already run

To send through an existing collector instead of the bundled one:

```bash
make deploy-cloud MEDIATOR=... \
  --set otelCollector.enabled=false \
  --set otel.endpoint=otel-collector.observability:4317
```

That collector **must** run the `k8sattributes` processor, or Causely drops every span — see
[causely-setup.md](causely-setup.md). To keep the bundled collector but also fan out elsewhere, add
an extra exporter instead:

```yaml
otelCollector:
  extraExporters:
    otlp/tempo:
      endpoint: tempo.observability:4317
      tls: { insecure: true }
```

## Appendix: cloud provider notes

Nothing in this demo is provider-specific. These are just the commands each provider needs for
kubeconfig and registry access.

### EKS

```bash
aws eks update-kubeconfig --region <region> --name <cluster>

# Only if you are publishing your own image to ECR. The repository must exist first.
aws ecr create-repository --repository-name tracey-shop --region <region>
aws ecr get-login-password --region <region> \
  | docker login --username AWS --password-stdin <account>.dkr.ecr.<region>.amazonaws.com
make push REGISTRY=<account>.dkr.ecr.<region>.amazonaws.com IMAGE_NAME=tracey-shop
```

Typical StorageClass: `gp3`. Some clusters still default to `gp2`, which does not allow volume
expansion.

### GKE

```bash
gcloud container clusters get-credentials <cluster> --region <region>

# Only if you are publishing your own image to Artifact Registry.
gcloud auth configure-docker <region>-docker.pkg.dev
make push REGISTRY=<region>-docker.pkg.dev IMAGE_NAME=<project>/<repo>/tracey-shop
```

Typical StorageClass: `standard-rwo`. If you point the demo at a Cloud SQL instance behind the auth
proxy, set `causelyIntegrations.postgres.hostOverwrite` to the real database address.

### AKS

```bash
az aks get-credentials --resource-group <group> --name <cluster>

# Only if you are publishing your own image to ACR.
az acr login --name <registry>
make push REGISTRY=<registry>.azurecr.io IMAGE_NAME=tracey-shop
```

Typical StorageClass: `managed-csi`.
