package obs

import (
	"context"
	"strings"
	"testing"

	"github.com/causely-oss/tracey-shop/internal/config"
)

// TestSetupBuildsAResource is the guard for a failure that took down every pod
// in the demo.
//
// resource.New merges our attributes with the SDK's own detectors and refuses to
// merge two different schema URLs. When an otel bump moved the SDK's detectors
// to semconv v1.43.0 while this package still declared v1.34.0, every role died
// at startup with:
//
//	setup telemetry: build resource: error detecting resource:
//	conflicting Schema URL: .../1.34.0 and .../1.43.0
//
// Nothing caught it, because no test had ever called Setup with an endpoint
// configured — the resource is only built once tracing is actually enabled.
func TestSetupBuildsAResource(t *testing.T) {
	t.Setenv("POD_NAME", "tracey-shop-catalog-api-abc123")
	t.Setenv("POD_UID", "8d1ab1bb-825b-428b-a3ff-5a76fff12524")
	t.Setenv("POD_NAMESPACE", "tracey-shop")
	t.Setenv("NODE_NAME", "ip-192-168-15-108")
	t.Setenv("CONTAINER_NAME", "shopd")

	cfg := &config.Config{
		ServiceName: "catalog-api",
		Namespace:   "tracey-shop",
		Version:     "0.1.1",
		Env:         "demo",
	}

	res, err := buildResource(context.Background(), cfg)
	if err != nil {
		t.Fatalf("buildResource: %v\n\nthis is the failure that CrashLoops every pod", err)
	}

	// The k8s attributes are what Causely resolves a span's workload from; a
	// span it cannot map is discarded silently, so their loss is invisible.
	got := map[string]string{}
	for _, kv := range res.Attributes() {
		got[string(kv.Key)] = kv.Value.AsString()
	}
	for key, want := range map[string]string{
		"service.name":       "catalog-api",
		"service.namespace":  "tracey-shop",
		"k8s.pod.name":       "tracey-shop-catalog-api-abc123",
		"k8s.pod.uid":        "8d1ab1bb-825b-428b-a3ff-5a76fff12524",
		"k8s.namespace.name": "tracey-shop",
	} {
		if got[key] != want {
			t.Errorf("resource %s = %q, want %q", key, got[key], want)
		}
	}
}

// TestSetupWithEndpointSucceeds exercises the whole path main() takes, which is
// where the schema-URL conflict actually surfaced.
func TestSetupWithEndpointSucceeds(t *testing.T) {
	t.Setenv("POD_UID", "8d1ab1bb-825b-428b-a3ff-5a76fff12524")

	// A non-routable endpoint is fine: the OTLP gRPC exporter connects lazily,
	// so Setup must still succeed without anything listening.
	cfg := &config.Config{
		ServiceName:  "catalog-api",
		Namespace:    "tracey-shop",
		Version:      "0.1.1",
		Env:          "demo",
		OTLPEndpoint: "127.0.0.1:4317",
		SampleRatio:  1.0,
	}

	shutdown, err := Setup(context.Background(), cfg)
	if err != nil {
		t.Fatalf("Setup: %v", err)
	}
	t.Cleanup(func() { _ = shutdown(context.Background()) })

	// And the schema URL must be the SDK's, or the merge above would have failed.
	res, err := buildResource(context.Background(), cfg)
	if err != nil {
		t.Fatalf("buildResource: %v", err)
	}
	if !strings.HasPrefix(res.SchemaURL(), "https://opentelemetry.io/schemas/") {
		t.Errorf("resource SchemaURL = %q, want an opentelemetry.io schema", res.SchemaURL())
	}
}
