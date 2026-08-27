#!/usr/bin/env bash
#
# Create a kind cluster sized for the demo.
#
# The whole stack is roughly 20 pods: 15 application services, a load generator,
# Postgres, Valkey, Kafka and an OTel collector. Budget ~6GB of RAM for the
# cluster, plus whatever Causely itself needs if you install it alongside.

set -euo pipefail

CLUSTER="${1:-tracey-demo}"
K8S_IMAGE="${K8S_IMAGE:-kindest/node:v1.33.1}"

log() { printf '\033[36m==>\033[0m %s\n' "$*"; }
die() { printf '\033[31merror\033[0m %s\n' "$*" >&2; exit 1; }

command -v kind >/dev/null || die "kind is required: https://kind.sigs.k8s.io"
command -v kubectl >/dev/null || die "kubectl is required"

if kind get clusters 2>/dev/null | grep -qx "$CLUSTER"; then
  log "cluster '$CLUSTER' already exists; reusing it"
  kubectl cluster-info --context "kind-${CLUSTER}" >/dev/null
  exit 0
fi

log "creating kind cluster '$CLUSTER'"

# Two workers so the demo's pods spread across nodes, which makes the
# k8s.node.name attribute meaningful in Causely's topology.
kind create cluster --name "$CLUSTER" --wait 120s --config=- <<EOF
kind: Cluster
apiVersion: kind.x-k8s.io/v1alpha4
nodes:
  - role: control-plane
    image: ${K8S_IMAGE}
  - role: worker
    image: ${K8S_IMAGE}
  - role: worker
    image: ${K8S_IMAGE}
EOF

log "cluster '$CLUSTER' ready"
kubectl get nodes

cat <<'NEXT'

Next:
  make kind-load    # build the image and side-load it
  make deploy       # install the chart

If Causely is not yet installed in this cluster, the collector will keep
retrying its exporter and log connection errors — the demo application itself
still runs fine.
NEXT
