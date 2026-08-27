package kafkax

import (
	"context"
	"testing"

	"github.com/IBM/sarama"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

// TestHeaderPropagationRoundTrip is what keeps the asynchronous half of the demo
// connected to the synchronous half. checkout-api publishes to `orders` and
// fraud-detector consumes it; if trace context does not survive the Kafka
// headers, fraud-detector and risk-model appear in Causely as orphaned trace
// roots rather than descendants of the checkout call.
func TestHeaderPropagationRoundTrip(t *testing.T) {
	otel.SetTextMapPropagator(propagation.TraceContext{})

	tp := sdktrace.NewTracerProvider(sdktrace.WithSampler(sdktrace.AlwaysSample()))
	defer func() { _ = tp.Shutdown(context.Background()) }()

	ctx, span := tp.Tracer("test").Start(context.Background(), "producing")
	defer span.End()
	want := span.SpanContext()

	// Producer side: inject into the outgoing message.
	msg := &sarama.ProducerMessage{Topic: "orders"}
	otel.GetTextMapPropagator().Inject(ctx, headerCarrier{msg: msg})

	if len(msg.Headers) == 0 {
		t.Fatal("no headers were injected; the consumer could not link its span")
	}

	// Hand the injected headers over as if they had come off the broker.
	consumed := &sarama.ConsumerMessage{Topic: "orders"}
	for _, h := range msg.Headers {
		consumed.Headers = append(consumed.Headers, &sarama.RecordHeader{Key: h.Key, Value: h.Value})
	}

	extracted := otel.GetTextMapPropagator().Extract(
		context.Background(), consumerHeaderCarrier{msg: consumed})
	got := trace.SpanContextFromContext(extracted)

	if !got.IsValid() {
		t.Fatal("extracted span context is invalid; the async chain would be disconnected")
	}
	if got.TraceID() != want.TraceID() {
		t.Errorf("trace id = %s, want %s", got.TraceID(), want.TraceID())
	}
	if got.SpanID() != want.SpanID() {
		t.Errorf("span id = %s, want %s", got.SpanID(), want.SpanID())
	}
}

func TestHeaderCarrierSetOverwritesExistingKey(t *testing.T) {
	msg := &sarama.ProducerMessage{}
	c := headerCarrier{msg: msg}

	c.Set("traceparent", "first")
	c.Set("traceparent", "second")

	if n := len(msg.Headers); n != 1 {
		t.Fatalf("expected the key to be overwritten in place, got %d headers", n)
	}
	if got := c.Get("traceparent"); got != "second" {
		t.Errorf("Get = %q, want %q", got, "second")
	}
	if keys := c.Keys(); len(keys) != 1 || keys[0] != "traceparent" {
		t.Errorf("Keys = %v, want [traceparent]", keys)
	}
}

func TestConsumerHeaderCarrierSkipsNilHeaders(t *testing.T) {
	// sarama can hand back nil entries in the header slice.
	msg := &sarama.ConsumerMessage{Headers: []*sarama.RecordHeader{
		nil,
		{Key: []byte("traceparent"), Value: []byte("value")},
		nil,
	}}
	c := consumerHeaderCarrier{msg: msg}

	if got := c.Get("traceparent"); got != "value" {
		t.Errorf("Get = %q, want %q", got, "value")
	}
	if got := c.Get("absent"); got != "" {
		t.Errorf("Get(absent) = %q, want empty", got)
	}
	if keys := c.Keys(); len(keys) != 1 {
		t.Errorf("Keys = %v, want exactly one non-nil key", keys)
	}
}

func TestSplitBrokerDefaultsToKafkaPort(t *testing.T) {
	cases := []struct {
		in       string
		wantHost string
		wantPort int
	}{
		{"tracey-shop-kafka:9092", "tracey-shop-kafka", 9092},
		{"kafka:19092", "kafka", 19092},
		// server.address must still be set even when the port is unparseable,
		// or Causely cannot resolve the broker and the topic edge is lost.
		{"kafka", "kafka", 9092},
	}
	for _, tc := range cases {
		host, port := splitBroker(tc.in)
		if host != tc.wantHost || port != tc.wantPort {
			t.Errorf("splitBroker(%q) = (%q, %d), want (%q, %d)",
				tc.in, host, port, tc.wantHost, tc.wantPort)
		}
	}
}
