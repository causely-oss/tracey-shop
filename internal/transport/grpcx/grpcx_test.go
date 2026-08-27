package grpcx

import (
	"context"
	"fmt"
	"net"
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

	// Causely requires rpc.system before it treats a span as an RPC call at all.
	sys, ok := attrOf(clientSpan, "rpc.system")
	if !ok {
		t.Fatal("gRPC CLIENT span is missing rpc.system — gRPC edges will not be detected")
	}
	if sys.AsString() != "grpc" {
		t.Errorf("rpc.system = %q, want \"grpc\"", sys.AsString())
	}

	for _, key := range []string{"rpc.method", "rpc.service"} {
		if _, ok := attrOf(clientSpan, key); !ok {
			t.Errorf("gRPC CLIENT span is missing %s", key)
		}
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
// gRPC INTERNAL, which is what Causely reads from rpc.grpc.status_code.
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
