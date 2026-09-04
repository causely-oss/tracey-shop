// Package obs wires up OpenTelemetry tracing and logging.
//
// The resource attributes and span attributes produced here are shaped
// deliberately to satisfy Causely's trace-ingest contract:
//
//   - Causely resolves a span's Kubernetes workload from k8s.pod.uid, or
//     k8s.namespace.name + k8s.pod.name, or container.id. If none resolve, the
//     span is discarded and the service never appears in the topology. The
//     collector's k8sattributes processor is the primary source of those, but we
//     also set them from the downward API so traces stay mappable even when
//     exported to a collector that lacks the processor.
//   - Topology edges come from span kind. Only SERVER, CLIENT, PRODUCER and
//     CONSUMER spans are analysed; INTERNAL spans are ignored (and dropped at
//     the collector), so anything that should show up as a dependency must be a
//     CLIENT/PRODUCER/CONSUMER span with peer attributes attached.
package obs

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.43.0"
	"go.opentelemetry.io/otel/trace"

	"github.com/causely-oss/tracey-shop/internal/config"
)

// ShutdownFunc flushes and stops the telemetry pipeline.
type ShutdownFunc func(context.Context) error

// Setup installs the global tracer provider and propagator, and returns a
// shutdown function. When cfg.OTLPEndpoint is empty tracing stays a no-op,
// which keeps the binary runnable outside a cluster.
func Setup(ctx context.Context, cfg *config.Config) (ShutdownFunc, error) {
	// W3C trace context so Kafka header propagation links async hops.
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))

	if cfg.OTLPEndpoint == "" {
		slog.Warn("OTEL_EXPORTER_OTLP_ENDPOINT is unset; tracing disabled")
		return func(context.Context) error { return nil }, nil
	}

	res, err := buildResource(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("build resource: %w", err)
	}

	exp, err := otlptracegrpc.New(ctx,
		otlptracegrpc.WithEndpoint(stripScheme(cfg.OTLPEndpoint)),
		otlptracegrpc.WithInsecure(),
		otlptracegrpc.WithTimeout(10*time.Second),
	)
	if err != nil {
		return nil, fmt.Errorf("create otlp exporter: %w", err)
	}

	sampler := sdktrace.ParentBased(sdktrace.TraceIDRatioBased(cfg.SampleRatio))
	if cfg.SampleRatio >= 1.0 {
		sampler = sdktrace.ParentBased(sdktrace.AlwaysSample())
	}

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithResource(res),
		sdktrace.WithSampler(sampler),
		sdktrace.WithBatcher(exp,
			sdktrace.WithBatchTimeout(2*time.Second),
			sdktrace.WithMaxQueueSize(4096),
			sdktrace.WithMaxExportBatchSize(1024),
		),
	)
	otel.SetTracerProvider(tp)

	return tp.Shutdown, nil
}

func buildResource(ctx context.Context, cfg *config.Config) (*resource.Resource, error) {
	attrs := []attribute.KeyValue{
		semconv.ServiceName(cfg.ServiceName),
		semconv.ServiceNamespace(cfg.Namespace),
		semconv.ServiceVersion(cfg.Version),
		semconv.DeploymentEnvironmentNameKey.String(cfg.Env),
	}

	// Downward-API values. These are what Causely's findWorkloadEntity uses to
	// map a span onto a Kubernetes workload; without one of them the span is
	// silently dropped on ingest.
	if v := os.Getenv("POD_NAME"); v != "" {
		attrs = append(attrs, semconv.K8SPodName(v))
	}
	if v := os.Getenv("POD_UID"); v != "" {
		attrs = append(attrs, semconv.K8SPodUID(v))
	}
	if v := os.Getenv("POD_NAMESPACE"); v != "" {
		attrs = append(attrs, semconv.K8SNamespaceName(v))
	}
	if v := os.Getenv("NODE_NAME"); v != "" {
		attrs = append(attrs, semconv.K8SNodeName(v))
	}
	if v := os.Getenv("CONTAINER_NAME"); v != "" {
		attrs = append(attrs, semconv.K8SContainerName(v))
	}

	// The semconv version here MUST match the one the SDK's own resource
	// detectors use (go.opentelemetry.io/otel/sdk/resource imports
	// semconv/v1.43.0 as of sdk v1.45.0). resource.New merges our attributes
	// with the detectors' and REFUSES to merge two different schema URLs:
	//
	//   error detecting resource: conflicting Schema URL:
	//     https://opentelemetry.io/schemas/1.34.0 and .../1.43.0
	//
	// That error is fatal in main(), so every pod CrashLoops — which is exactly
	// what an otel bump did to this repo. TestSetupBuildsAResource guards it.
	//
	// Only this file needs to move in step. The transport packages pin
	// semconv/v1.34.0 for SPAN attributes, which is unrelated: the resource
	// schema URL describes the resource, not the spans. internal/genai depends
	// on that pin specifically, because the experimental gen_ai group was
	// dropped from the v1.43.0 package.
	return resource.New(ctx,
		resource.WithSchemaURL(semconv.SchemaURL),
		resource.WithProcessRuntimeName(),
		resource.WithProcessRuntimeVersion(),
		resource.WithTelemetrySDK(),
		resource.WithAttributes(attrs...),
	)
}

// stripScheme normalises an endpoint for otlptracegrpc, which wants host:port.
func stripScheme(ep string) string {
	ep = strings.TrimPrefix(ep, "http://")
	ep = strings.TrimPrefix(ep, "https://")
	return strings.TrimSuffix(ep, "/")
}

// Tracer returns the demo's tracer.
func Tracer(name string) trace.Tracer {
	return otel.Tracer("github.com/causely-oss/tracey-shop/" + name)
}

// SetupLogging configures slog with the requested level.
func SetupLogging(cfg *config.Config) {
	level := slog.LevelInfo
	switch strings.ToLower(cfg.LogLevel) {
	case "debug":
		level = slog.LevelDebug
	case "warn", "warning":
		level = slog.LevelWarn
	case "error":
		level = slog.LevelError
	}
	h := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: level})
	slog.SetDefault(slog.New(h).With(
		slog.String("service", cfg.ServiceName),
		slog.String("role", cfg.Role),
	))
}

// LogTraceCtx returns log attributes carrying the current trace/span ids, so
// logs can be correlated with traces in Causely.
func LogTraceCtx(ctx context.Context) []any {
	sc := trace.SpanContextFromContext(ctx)
	if !sc.IsValid() {
		return nil
	}
	return []any{
		slog.String("trace_id", sc.TraceID().String()),
		slog.String("span_id", sc.SpanID().String()),
	}
}
