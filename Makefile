# Tracey Shop — Causely demo application.

SHELL := /bin/bash
.DEFAULT_GOAL := help

TAG          ?= dev
CHART        ?= deploy/tracey-shop
RELEASE      ?= tracey-shop
NAMESPACE    ?= tracey-shop
KIND_CLUSTER ?= tracey-demo
VALUES       ?= $(CHART)/values-kind.yaml

# Mediator endpoint the collector exports to.
#
# `mediator.causely:4317` is where a standard Causely install puts the mediator.
# A wrong endpoint fails silently — the pods stay healthy and only the collector's
# exporter logs complain — so confirm yours first:
#
#   make mediators
MEDIATOR          ?= mediator.causely:4317
MEDIATOR_PROTOCOL ?= grpc

# --- Image / registry --------------------------------------------------------
#
# Defaults match .github/workflows/release.yaml and the chart's default
# image.repository, so nothing needs building to run the demo. To publish your
# own somewhere else:
#
#   docker login <registry>
#   make push REGISTRY=quay.io IMAGE_NAME=myorg/tracey-shop TAG=v0.1.0
#
# This Makefile does not manage registry credentials — log in yourself.
REGISTRY   ?= ghcr.io
IMAGE_NAME ?= causely-oss/tracey-shop
IMAGE_REPO ?= $(REGISTRY)/$(IMAGE_NAME)
IMAGE_REF   = $(IMAGE_REPO):$(TAG)

# The local build loop: built, side-loaded into kind, never pushed.
LOCAL_IMAGE ?= tracey-shop
PULL_POLICY ?= Never

# Tag `make deploy-cloud` pulls. Empty means the chart default, its appVersion,
# which is the tag the release workflow publishes.
DEPLOY_TAG ?=

# Multi-arch by default. A node group's architecture can change without warning,
# and a single-arch image then fails every pull with "no match for platform in
# manifest". Building both costs seconds, because the Dockerfile cross-compiles
# with Go rather than emulating the target under QEMU.
PLATFORMS      ?= linux/amd64,linux/arm64
BUILDX_BUILDER ?= tracey-shop-builder
# Local single-arch builds (make build / kind-load) stay on the host's arch.
PLATFORM       ?=

.PHONY: help
help: ## Show this help
	@grep -hE '^[a-zA-Z0-9_-]+:.*?## ' $(MAKEFILE_LIST) \
		| awk 'BEGIN{FS=":.*?## "}{printf "  \033[36m%-16s\033[0m %s\n", $$1, $$2}'

# ---------------------------------------------------------------------------
# Go
# ---------------------------------------------------------------------------

.PHONY: proto-tools
proto-tools: ## Install the protoc plugins `make proto` needs
	go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.36.6
	go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.5.1

.PHONY: proto
proto: ## Regenerate protobuf/gRPC stubs into gen/ (needs `make proto-tools`)
	protoc \
		--go_out=. --go_opt=module=github.com/causely-oss/tracey-shop \
		--go-grpc_out=. --go-grpc_opt=module=github.com/causely-oss/tracey-shop \
		proto/shop/v1/shop.proto

.PHONY: tidy
tidy: ## go mod tidy
	go mod tidy

.PHONY: fmt
fmt: ## gofmt all hand-written sources
	gofmt -w $$(find . -name '*.go' -not -path './gen/*')

.PHONY: lint
lint: ## go vet + helm lint
	go vet ./...
	helm lint $(CHART)

.PHONY: test
test: ## Run unit tests
	go test ./...

.PHONY: compile
compile: ## Compile the binary locally
	go build ./...

# ---------------------------------------------------------------------------
# Image
# ---------------------------------------------------------------------------

.PHONY: build
build: ## Build the container image locally
	docker build -t $(LOCAL_IMAGE):$(TAG) .

.PHONY: kind-up
kind-up: ## Create the kind cluster
	./scripts/kind-up.sh $(KIND_CLUSTER)

.PHONY: kind-load
kind-load: build ## Build and side-load the image into kind
	kind load docker-image $(LOCAL_IMAGE):$(TAG) --name $(KIND_CLUSTER)

# ---------------------------------------------------------------------------
# Publishing
# ---------------------------------------------------------------------------

.PHONY: buildx-builder
buildx-builder: ## Create the buildx builder used for multi-arch pushes
	@docker buildx inspect $(BUILDX_BUILDER) >/dev/null 2>&1 \
		|| docker buildx create --name $(BUILDX_BUILDER) --driver docker-container --bootstrap

.PHONY: push
push: buildx-builder ## Build multi-arch and push to $(IMAGE_REPO)
	docker buildx build \
		--builder $(BUILDX_BUILDER) \
		--platform $(PLATFORMS) \
		-t $(IMAGE_REF) \
		--push .
	@echo
	@echo "Pushed $(IMAGE_REF) for $(PLATFORMS)"
	@docker buildx imagetools inspect $(IMAGE_REF) \
		| grep -E 'Platform|Name:' | sed 's/^/  /'

# ---------------------------------------------------------------------------
# Chart
# ---------------------------------------------------------------------------

# Renders exactly what `make deploy` would install, so the two cannot disagree.
.PHONY: template
template: ## Render the local-build chart to stdout
	helm template $(RELEASE) $(CHART) -n $(NAMESPACE) \
		-f $(VALUES) \
		--set image.repository=$(LOCAL_IMAGE) \
		--set image.tag=$(TAG) \
		--set image.pullPolicy=$(PULL_POLICY) \
		--set otelCollector.exporter.endpoint=$(MEDIATOR) \
		--set otelCollector.exporter.protocol=$(MEDIATOR_PROTOCOL)

# The local build loop: deploys the image `make kind-load` side-loaded, which is
# why pullPolicy is Never. For the published image, use `make deploy-cloud`.
.PHONY: deploy
deploy: ## Install or upgrade using a locally built, side-loaded image
	helm upgrade --install $(RELEASE) $(CHART) \
		-n $(NAMESPACE) --create-namespace \
		-f $(VALUES) \
		--set image.repository=$(LOCAL_IMAGE) \
		--set image.tag=$(TAG) \
		--set image.pullPolicy=$(PULL_POLICY) \
		--set otelCollector.exporter.endpoint=$(MEDIATOR) \
		--set otelCollector.exporter.protocol=$(MEDIATOR_PROTOCOL) \
		--wait --timeout 10m

.PHONY: deploy-cloud
deploy-cloud: ## Install or upgrade on a real cluster, pulling the published image
	@echo "context:  $$(kubectl config current-context)"
	@echo "image:    $(IMAGE_REPO):$(if $(DEPLOY_TAG),$(DEPLOY_TAG),$$(helm show chart $(CHART) | awk '/^appVersion:/ {print $$2}' | tr -d '\"'))"
	@echo "mediator: $(MEDIATOR) ($(MEDIATOR_PROTOCOL))"
	@echo
	helm upgrade --install $(RELEASE) $(CHART) \
		-n $(NAMESPACE) --create-namespace \
		-f $(CHART)/values-cloud.yaml \
		--set image.repository=$(IMAGE_REPO) \
		$(if $(DEPLOY_TAG),--set image.tag=$(DEPLOY_TAG),) \
		--set image.pullPolicy=IfNotPresent \
		--set otelCollector.exporter.endpoint=$(MEDIATOR) \
		--set otelCollector.exporter.protocol=$(MEDIATOR_PROTOCOL) \
		--wait --timeout 15m

.PHONY: template-cloud
template-cloud: ## Render the chart as `make deploy-cloud` would install it
	helm template $(RELEASE) $(CHART) -n $(NAMESPACE) \
		-f $(CHART)/values-cloud.yaml \
		--set image.repository=$(IMAGE_REPO) \
		$(if $(DEPLOY_TAG),--set image.tag=$(DEPLOY_TAG),) \
		--set image.pullPolicy=IfNotPresent \
		--set otelCollector.exporter.endpoint=$(MEDIATOR)

.PHONY: mediators
mediators: ## List the mediator endpoints available in the current cluster
	@echo "Mediator services reachable in this cluster:"
	@kubectl get svc -A -o jsonpath='{range .items[*]}{.metadata.namespace}{" "}{.metadata.name}{" "}{.spec.ports[*].port}{"\n"}{end}' \
		| awk '$$2 == "mediator" { printf "  mediator.%s:4317\n", $$1 }'
	@echo
	@echo "Pass one with: make deploy-cloud MEDIATOR=mediator.<namespace>:4317"

.PHONY: undeploy
undeploy: ## Uninstall the release
	helm uninstall $(RELEASE) -n $(NAMESPACE) || true
	kubectl delete namespace $(NAMESPACE) --ignore-not-found

.PHONY: redeploy
redeploy: kind-load deploy ## Rebuild, reload and upgrade in one step
	kubectl -n $(NAMESPACE) rollout restart deployment -l app.kubernetes.io/part-of=tracey-shop

# ---------------------------------------------------------------------------
# Operating the demo
# ---------------------------------------------------------------------------

.PHONY: status
status: ## Show pods and restart counts
	kubectl -n $(NAMESPACE) get pods -o wide

.PHONY: logs
logs: ## Tail every application pod
	kubectl -n $(NAMESPACE) logs -l app.kubernetes.io/part-of=tracey-shop --tail=50 -f --max-log-requests=25

.PHONY: collector-logs
collector-logs: ## Tail the OTel collector
	kubectl -n $(NAMESPACE) logs deploy/$(RELEASE)-otel-collector -f

.PHONY: verify
verify: ## Check the trace attributes Causely requires
	./scripts/verify-traces.sh

.PHONY: verify-db
verify-db: ## Check the Causely PostgreSQL integration
	./scripts/verify-db-integration.sh

.PHONY: verify-lag
verify-lag: ## Check the Kafka consumer-lag pipeline
	./scripts/verify-lag-integration.sh

.PHONY: scenarios
scenarios: ## List the fault scenarios
	./scripts/scenario.sh list

.PHONY: port-forward
port-forward: ## Expose the storefront on localhost:8080
	kubectl -n $(NAMESPACE) port-forward svc/$(RELEASE)-storefront-bff 8080:8080
