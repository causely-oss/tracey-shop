package grpcx

import (
	"context"
	"fmt"
	"net"
	"strings"
	"testing"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"

	"github.com/causely-oss/tracey-shop/internal/faults"
)

func setupRecorder(t *testing.T) *tracetest.SpanRecorder {
	t.Helper()
	rec := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
		sdktrace.WithSpanProcessor(rec),
	)
	prev := otel.GetTracerProvider()
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.TraceContext{})
	t.Cleanup(func() {
		_ = tp.Shutdown(context.Background())
		otel.SetTracerProvider(prev)
	})
	return rec
}

func attrOf(span sdktrace.ReadOnlySpan, key string) (attribute.Value, bool) {
	for _, kv := range span.Attributes() {
		if string(kv.Key) == key {
			return kv.Value, true
		}
	}
	return attribute.Value{}, false
}

// startServer boots a grpcx server on a free port. grpcx.NewServer already
// registers the gRPC health service, which gives us a real RPC to call without
// needing a test-only proto.
func startServer(t *testing.T) (port int, stop func()) {
	t.Helper()

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	port = lis.Addr().(*net.TCPAddr).Port
	if err := lis.Close(); err != nil {
		t.Fatalf("close probe listener: %v", err)
	}

	srv := NewServer(fmt.Sprintf("127.0.0.1:%d", port), faults.NewStore("test-svc"))
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = srv.Start(ctx)
	}()

	// Wait for the listener to accept.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		c, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", port), 200*time.Millisecond)
		if err == nil {
			_ = c.Close()
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	return port, func() {
		cancel()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
		}
	}
}

// TestClientSpanPinsDNSPeerAddress is the reason peerPinHandler exists.
//
// otelgrpc sets server.address from grpc's resolved peer, which in Kubernetes is
// an IP rather than the Service DNS name. Causely can resolve either, but the
// hostname path is the well-trodden one — it indexes Services by hostname and
// retries a short name as <name>.<caller's namespace>. This test dials by
// hostname while the connection resolves to 127.0.0.1, so it fails if the pin
// stops winning against otelgrpc's overwrite.
func TestClientSpanPinsDNSPeerAddress(t *testing.T) {
	rec := setupRecorder(t)

	port, stop := startServer(t)
	defer stop()

	target := fmt.Sprintf("localhost:%d", port)
	conn, err := Dial(target)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if _, err := healthpb.NewHealthClient(conn).Check(ctx, &healthpb.HealthCheckRequest{}); err != nil {
		t.Fatalf("health check: %v", err)
	}

	var clientSpan sdktrace.ReadOnlySpan
	for _, s := range rec.Ended() {
		if s.SpanKind() == trace.SpanKindClient {
			clientSpan = s
			break
		}
	}
	if clientSpan == nil {
		t.Fatal("no CLIENT span recorded for the gRPC call")
	}

	addr, ok := attrOf(clientSpan, "server.address")
	if !ok {
		t.Fatal("gRPC CLIENT span is missing server.address — no dependency edge would be built")
	}
	if addr.AsString() != "localhost" {
		t.Errorf("server.address = %q, want %q (the DNS target). "+
			"otelgrpc likely overwrote it with the resolved peer IP, which means "+
			"peerPinHandler is no longer winning.", addr.AsString(), "localhost")
	}

	gotPort, ok := attrOf(clientSpan, "server.port")
	if !ok {
		t.Error("gRPC CLIENT span is missing server.port")
	} else if int(gotPort.AsInt64()) != port {
		t.Errorf("server.port = %d, want %d", gotPort.AsInt64(), port)
	}

	// Causely requires the rpc system attribute before it treats a span as an
	// RPC call at all, so its absence costs every gRPC edge in the topology.
	//
	// otelgrpc v0.70.0 moved to the semconv 1.43 spelling: rpc.system became
	// rpc.system.name. Causely's mediator reads both, and this demo tracks the
	// current convention rather than opting back into the old one with
	// OTEL_SEMCONV_STABILITY_OPT_IN=rpc/dup.
	sys, ok := attrOf(clientSpan, "rpc.system.name")
	if !ok {
		t.Fatal("gRPC CLIENT span is missing rpc.system.name — gRPC edges will not be detected")
	}
	if sys.AsString() != "grpc" {
		t.Errorf("rpc.system.name = %q, want \"grpc\"", sys.AsString())
	}

	// The same change dropped rpc.service and made rpc.method the fully
	// qualified name. That is load-bearing rather than cosmetic: Causely builds
	// an RPCMethod entity named "<rpc.service>/<rpc.method>", and falls back to
	// rpc.method alone when rpc.service is absent — so the entity keeps the
	// exact same display name, e.g. shop.v1.PaymentService/Authorize.
	//
	// Asserting the shape here is what stops a future bump from silently
	// renaming every RPCMethod entity in the demo.
	method, ok := attrOf(clientSpan, "rpc.method")
	if !ok {
		t.Fatal("gRPC CLIENT span is missing rpc.method")
	}
	if !strings.Contains(method.AsString(), "/") {
		t.Errorf("rpc.method = %q, want the fully qualified <service>/<method> — "+
			"Causely would otherwise name the RPCMethod entity with the bare method",
			method.AsString())
	}

	// Deliberately asserted absent. If a future otelgrpc starts emitting
	// rpc.service again alongside a fully qualified rpc.method, Causely would
	// name the entity "<service>/<service>/<method>".
	if svc, present := attrOf(clientSpan, "rpc.service"); present {
		t.Errorf("rpc.service is present (%q) alongside a fully qualified rpc.method; "+
			"Causely would double up the RPCMethod entity name", svc.AsString())
	}

	// The old spelling must be gone, since we are not opting into rpc/dup.
	if _, present := attrOf(clientSpan, "rpc.system"); present {
		t.Error("rpc.system is still present; the old convention was expected to be dropped")
	}

	// The gRPC status, which is what every gRPC error rate in the demo is
	// computed from. otelgrpc v0.70 replaced the numeric rpc.grpc.status_code
	// with this string form. The mediator counts a failure when either the
	// string is in its RCP_ERROR_CODES or the number is in GRCP_ERROR_CODES, so
	// losing both would silently understate every gRPC error rate — which is
	// what payment-errors and the order-intake backpressure both depend on.
	status, ok := attrOf(clientSpan, "rpc.response.status_code")
	if !ok {
		if _, old := attrOf(clientSpan, "rpc.grpc.status_code"); !old {
			t.Fatal("gRPC CLIENT span carries neither rpc.response.status_code nor " +
				"rpc.grpc.status_code — gRPC error rates would be understated")
		}
	} else if status.AsString() != "OK" {
		t.Errorf("rpc.response.status_code = %q on a healthy call, want \"OK\"", status.AsString())
	}
}

// TestServerSpanRecordedAndTraceLinked confirms the inbound gRPC side produces a
// SERVER span in the same trace, so the call graph stays connected.
func TestServerSpanRecordedAndTraceLinked(t *testing.T) {
	rec := setupRecorder(t)

	port, stop := startServer(t)
	defer stop()

	conn, err := Dial(fmt.Sprintf("localhost:%d", port))
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := healthpb.NewHealthClient(conn).Check(ctx, &healthpb.HealthCheckRequest{}); err != nil {
		t.Fatalf("health check: %v", err)
	}

	var client, server sdktrace.ReadOnlySpan
	for _, s := range rec.Ended() {
		switch s.SpanKind() {
		case trace.SpanKindClient:
			if client == nil {
				client = s
			}
		case trace.SpanKindServer:
			if server == nil {
				server = s
			}
		}
	}
	if client == nil {
		t.Fatal("no CLIENT span recorded")
	}
	if server == nil {
		t.Fatal("no SERVER span recorded — the service would have no latency or error metrics")
	}
	if client.SpanContext().TraceID() != server.SpanContext().TraceID() {
		t.Error("client and server spans are in different traces; context did not propagate")
	}
}

// TestFaultInterceptorReturnsInternal verifies an injected fault surfaces as
// gRPC INTERNAL, which Causely reads from rpc.response.status_code.
func TestFaultInterceptorReturnsInternal(t *testing.T) {
	setupRecorder(t)

	store := faults.NewStore("test-svc")
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	port := lis.Addr().(*net.TCPAddr).Port
	_ = lis.Close()

	srv := NewServer(fmt.Sprintf("127.0.0.1:%d", port), store)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = srv.Start(ctx) }()

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		c, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", port), 200*time.Millisecond)
		if err == nil {
			_ = c.Close()
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	conn, err := Dial(fmt.Sprintf("localhost:%d", port))
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer conn.Close()

	client := healthpb.NewHealthClient(conn)
	callCtx, callCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer callCancel()

	// Healthy first.
	if _, err := client.Check(callCtx, &healthpb.HealthCheckRequest{}); err != nil {
		t.Fatalf("health check before fault injection: %v", err)
	}

	rate := 1.0
	store.Apply(faults.Patch{ErrorRate: &rate})

	_, err = client.Check(callCtx, &healthpb.HealthCheckRequest{})
	if err == nil {
		t.Fatal("expected the injected fault to fail the RPC")
	}

	// And clearing it restores service without a restart, which is what makes
	// scenarios repeatable mid-demo.
	store.Clear()
	if _, err := client.Check(callCtx, &healthpb.HealthCheckRequest{}); err != nil {
		t.Fatalf("health check after clearing faults: %v", err)
	}
}
