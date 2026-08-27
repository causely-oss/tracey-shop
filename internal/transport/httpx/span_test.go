package httpx

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"

	"github.com/causely-oss/tracey-shop/internal/faults"
)

// setupRecorder installs a real SDK tracer provider that records spans in
// memory, so these tests exercise the same code path production does.
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

func findSpan(t *testing.T, rec *tracetest.SpanRecorder, kind trace.SpanKind) sdktrace.ReadOnlySpan {
	t.Helper()
	for _, s := range rec.Ended() {
		if s.SpanKind() == kind {
			return s
		}
	}
	var kinds []string
	for _, s := range rec.Ended() {
		kinds = append(kinds, s.SpanKind().String()+":"+s.Name())
	}
	t.Fatalf("no %s span recorded; got %v", kind, kinds)
	return nil
}

// TestClientSpanCarriesPeerAttributes is the test that matters most in this
// package. Causely resolves an HTTP dependency edge from server.address on the
// CLIENT span; if that attribute is absent or wrong, the edge simply does not
// appear in the topology and the demo looks broken from the Causely side.
func TestClientSpanCarriesPeerAttributes(t *testing.T) {
	rec := setupRecorder(t)

	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer upstream.Close()

	client := NewClient(upstream.URL, 5*time.Second, faults.NewStore("test-svc"))

	var out map[string]bool
	if err := client.GetJSON(context.Background(), "/carts/abc", &out); err != nil {
		t.Fatalf("GetJSON: %v", err)
	}
	if !out["ok"] {
		t.Fatalf("unexpected response body: %v", out)
	}

	span := findSpan(t, rec, trace.SpanKindClient)

	wantHost, wantPort := hostPortFromURL(upstream.URL)

	got, ok := attrOf(span, "server.address")
	if !ok {
		t.Fatal("CLIENT span is missing server.address — Causely cannot resolve the dependency edge")
	}
	if got.AsString() != wantHost {
		t.Errorf("server.address = %q, want %q", got.AsString(), wantHost)
	}

	gotPort, ok := attrOf(span, "server.port")
	if !ok {
		t.Fatal("CLIENT span is missing server.port")
	}
	if int(gotPort.AsInt64()) != wantPort {
		t.Errorf("server.port = %d, want %d", gotPort.AsInt64(), wantPort)
	}

	// Drives Causely's error-rate symptom for HTTP dependencies.
	if _, ok := attrOf(span, "http.response.status_code"); !ok {
		t.Error("CLIENT span is missing http.response.status_code")
	}
	if _, ok := attrOf(span, "http.request.method"); !ok {
		t.Error("CLIENT span is missing http.request.method")
	}
}

// TestServerSpanIsRecordedAndErrorsAreMarked checks the inbound side: a SERVER
// span must exist for latency/error metrics, and an injected fault must set the
// span status to Error, which is what Causely counts.
func TestServerSpanIsRecordedAndErrorsAreMarked(t *testing.T) {
	rec := setupRecorder(t)

	store := faults.NewStore("test-svc")
	rate := 1.0
	store.Apply(faults.Patch{ErrorRate: &rate})

	srv := NewServer("test-svc", "127.0.0.1:0", store)
	srv.Route("GET /api/things", func(ctx context.Context, r *http.Request) (any, error) {
		return map[string]string{"unreachable": "true"}, nil
	})

	ts := httptest.NewServer(srv.mux)
	defer ts.Close()

	resp, err := http.Get(ts.URL + "/api/things")
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500 (the errorRate fault should have fired)", resp.StatusCode)
	}

	span := findSpan(t, rec, trace.SpanKindServer)
	if span.Status().Code.String() != "Error" {
		t.Errorf("SERVER span status = %s, want Error — Causely counts span status for error rate",
			span.Status().Code)
	}
	if len(span.Events()) == 0 {
		t.Error("SERVER span has no exception event; RecordError was not called")
	}
}

// TestServerSpanNameUsesRoute keeps span names bounded. Naming spans after the
// raw path would create an unbounded set of HTTP endpoint entities in Causely,
// one per cart id.
func TestServerSpanNameUsesRoute(t *testing.T) {
	rec := setupRecorder(t)

	srv := NewServer("test-svc", "127.0.0.1:0", faults.NewStore("test-svc"))
	srv.Route("GET /carts/{id}", func(ctx context.Context, r *http.Request) (any, error) {
		return map[string]string{"id": r.PathValue("id")}, nil
	})

	ts := httptest.NewServer(srv.mux)
	defer ts.Close()

	for _, id := range []string{"cart-1", "cart-2", "cart-3"} {
		resp, err := http.Get(ts.URL + "/carts/" + id)
		if err != nil {
			t.Fatalf("GET %s: %v", id, err)
		}
		_ = resp.Body.Close()
	}

	names := map[string]int{}
	for _, s := range rec.Ended() {
		if s.SpanKind() == trace.SpanKindServer {
			names[s.Name()]++
		}
	}
	if len(names) != 1 {
		t.Errorf("expected one span name for three cart ids, got %v", names)
	}
	if _, ok := names["GET /carts/{id}"]; !ok {
		t.Errorf("span name should be the route template, got %v", names)
	}
}

// TestTraceContextPropagatesToServer confirms a single trace spans the hop, so
// Causely sees a connected call graph rather than disconnected roots.
func TestTraceContextPropagatesToServer(t *testing.T) {
	rec := setupRecorder(t)

	srv := NewServer("downstream", "127.0.0.1:0", faults.NewStore("test-svc"))
	srv.Route("GET /ping", func(ctx context.Context, r *http.Request) (any, error) {
		return map[string]string{"pong": "yes"}, nil
	})
	ts := httptest.NewServer(srv.mux)
	defer ts.Close()

	client := NewClient(ts.URL, 5*time.Second, faults.NewStore("test-svc"))
	var out map[string]string
	if err := client.GetJSON(context.Background(), "/ping", &out); err != nil {
		t.Fatalf("GetJSON: %v", err)
	}

	clientSpan := findSpan(t, rec, trace.SpanKindClient)
	serverSpan := findSpan(t, rec, trace.SpanKindServer)

	if clientSpan.SpanContext().TraceID() != serverSpan.SpanContext().TraceID() {
		t.Error("client and server spans have different trace ids; context did not propagate")
	}
	if serverSpan.Parent().SpanID() != clientSpan.SpanContext().SpanID() {
		t.Error("server span is not a child of the client span")
	}
}
