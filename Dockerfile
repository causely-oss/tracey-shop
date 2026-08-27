# Single image for every role in the demo. The ROLE environment variable
# selects which service the container runs; see cmd/shopd/main.go.
#
# Built multi-arch (amd64 + arm64), because a cluster's node group can change
# architecture under you, and an image built for only one arch then fails every
# pull with "no match for platform in manifest" — leaving pods in
# ImagePullBackOff and the Helm release failed, with nothing in the application
# logs to explain it.
ARG GO_VERSION=1.25

# --platform=$BUILDPLATFORM keeps the toolchain native and lets Go cross-compile,
# which is far faster than emulating the target arch under QEMU — and needs no
# binfmt setup on the build host. Safe here because the build is pure Go with
# CGO disabled.
FROM --platform=$BUILDPLATFORM golang:${GO_VERSION}-alpine AS build
WORKDIR /src

# Supplied by buildx for each target platform.
ARG TARGETOS
ARG TARGETARCH

# Dependencies first, so a code-only change reuses the module layer.
COPY go.mod go.sum ./
RUN go mod download

COPY . .

# Static binary: nothing in the demo needs cgo, and a static binary runs on the
# distroless static base below.
RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} go build \
    -trimpath \
    -ldflags="-s -w" \
    -o /out/shopd \
    ./cmd/shopd

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/shopd /shopd
USER nonroot:nonroot

# Admin port: health probes and the fault-injection API. Business ports vary by
# role and are set through HTTP_ADDR / GRPC_ADDR.
EXPOSE 8090

ENTRYPOINT ["/shopd"]
