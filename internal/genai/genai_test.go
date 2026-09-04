package genai

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"

	"github.com/causely-oss/tracey-shop/internal/config"
	"github.com/causely-oss/tracey-shop/internal/faults"
)

// These tests are the regression guard for Causely's genAI ingest contract.
//
// Each requirement they assert is one the mediator fails silently — the span is
// simply never turned into an AIModel entity, with no error anywhere — so a
// regression here would surface only as "the demo stopped showing AI entities"
// long after the change that caused it. The contract is documented in the
// package comment and in docs/genai.md.

func setupRecorder(t *testing.T) *tracetest.SpanRecorder {
	t.Helper()
	rec := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
		sdktrace.WithSpanProcessor(rec),
	)
	prev := otel.GetTracerProvider()
	otel.SetTracerProvider(tp)
	t.Cleanup(func() {
		_ = tp.Shutdown(context.Background())
		otel.SetTracerProvider(prev)
	})
	return rec
}

func attrOf(t *testing.T, span sdktrace.ReadOnlySpan, key string) attribute.Value {
	t.Helper()
	for _, kv := range span.Attributes() {
		if string(kv.Key) == key {
			return kv.Value
		}
	}
	var got []string
	for _, kv := range span.Attributes() {
		got = append(got, string(kv.Key))
	}
	t.Fatalf("span %q is missing attribute %q; has %v", span.Name(), key, got)
	return attribute.Value{}
}

// clientFor builds a Client pointed at srv, with tracing already recording.
func clientFor(t *testing.T, srv *httptest.Server, api string) *Client {
	t.Helper()
	cfg := &config.Config{
		GenAIAPI:         api,
		GenAIBaseURL:     srv.URL,
		GenAIModel:       "gpt-4o-mini",
		GenAISystem:      "openai",
		GenAIAPIKey:      "test-key",
		GenAIMaxTokens:   256,
		GenAITemperature: 0.2,
		GenAITimeout:     5 * time.Second,
	}
	if api == config.APIAnthropic {
		cfg.GenAISystem = "anthropic"
		cfg.GenAIModel = "claude-haiku-4-5"
	}
	return New(cfg, faults.NewStore("ai-assistant"))
}

func openAIServer(t *testing.T, status int) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.URL.Path; got != "/v1/chat/completions" {
			t.Errorf("path = %q, want /v1/chat/completions", got)
		}
		if got := r.Header.Get("Authorization"); got != "Bearer test-key" {
			t.Errorf("Authorization = %q, want the bearer token", got)
		}
		if status != http.StatusOK {
			w.WriteHeader(status)
			_, _ = w.Write([]byte(`{"error":{"message":"nope"}}`))
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{
			"id": "chatcmpl-abc",
			"model": "gpt-4o-mini-2024-07-18",
			"choices": [{"message": {"role": "assistant", "content": " hello "}, "finish_reason": "stop"}],
			"usage": {"prompt_tokens": 2041, "completion_tokens": 29}
		}`))
	}))
	t.Cleanup(srv.Close)
	return srv
}

// TestChatSpanSatisfiesCauselyContract is the important one: it asserts every
// attribute analyzeGenAISpan needs, and the span kind it is only ever reached
// from.
func TestChatSpanSatisfiesCauselyContract(t *testing.T) {
	rec := setupRecorder(t)
	c := clientFor(t, openAIServer(t, http.StatusOK), config.APIOpenAI)

	res, err := c.Chat(context.Background(), "how long is delivery?")
	if err != nil {
		t.Fatalf("Chat: %v", err)
	}
	if res.Text != "hello" {
		t.Errorf("Text = %q, want %q (trimmed)", res.Text, "hello")
	}

	ended := rec.Ended()
	if len(ended) != 1 {
		// Requirement 1, the other half: exactly ONE span per inference. A
		// second CLIENT span here would mean the otelhttp transport crept back
		// in, which also makes Causely build a stray HTTPPath entity on the
		// provider Service.
		var names []string
		for _, s := range ended {
			names = append(names, s.SpanKind().String()+":"+s.Name())
		}
		t.Fatalf("recorded %d spans, want exactly 1; got %v", len(ended), names)
	}
	span := ended[0]

	// Requirement 1: genAI is only analysed in the mediator's client pass.
	if span.SpanKind() != trace.SpanKindClient {
		t.Errorf("span kind = %s, want Client", span.SpanKind())
	}

	// Requirement 5: the semconv "<operation> <model>" name.
	if want := "chat gpt-4o-mini"; span.Name() != want {
		t.Errorf("span name = %q, want %q", span.Name(), want)
	}

	// Requirement 2: presence is the sole trigger that classifies the span as
	// genAI. Both spellings, because the attribute was renamed in semconv 1.37.
	for _, key := range []string{"gen_ai.system", "gen_ai.provider.name"} {
		if got := attrOf(t, span, key).AsString(); got != "openai" {
			t.Errorf("%s = %q, want %q", key, got, "openai")
		}
	}

	// Requirement 3: without server.address the mediator returns immediately.
	if got := attrOf(t, span, "server.address").AsString(); got == "" {
		t.Error("server.address is empty — no AIModel would be created")
	}
	if got := attrOf(t, span, "server.port").AsInt64(); got == 0 {
		t.Error("server.port is 0")
	}

	// Requirement 4: an empty model is rejected outright.
	if got := attrOf(t, span, "gen_ai.request.model").AsString(); got != "gpt-4o-mini" {
		t.Errorf("gen_ai.request.model = %q, want gpt-4o-mini", got)
	}
	if got := attrOf(t, span, "gen_ai.response.model").AsString(); got != "gpt-4o-mini-2024-07-18" {
		t.Errorf("gen_ai.response.model = %q, want the resolved model", got)
	}

	// Requirement 5.
	if got := attrOf(t, span, "gen_ai.operation.name").AsString(); got != "chat" {
		t.Errorf("gen_ai.operation.name = %q, want chat", got)
	}

	// Requirement 6: the mediator reads IntValue only, so a Float64 here would
	// silently be read as zero tokens.
	for key, want := range map[string]int64{
		"gen_ai.usage.input_tokens":  2041,
		"gen_ai.usage.output_tokens": 29,
	} {
		v := attrOf(t, span, key)
		if v.Type() != attribute.INT64 {
			t.Errorf("%s type = %s, want INT64 (anything else reads as 0)", key, v.Type())
		}
		if v.AsInt64() != want {
			t.Errorf("%s = %d, want %d", key, v.AsInt64(), want)
		}
	}

	// Requirement 7: the only source of error and rate-limit signal.
	v := attrOf(t, span, "http.response.status_code")
	if v.Type() != attribute.INT64 || v.AsInt64() != 200 {
		t.Errorf("http.response.status_code = %v (%s), want Int 200", v.Emit(), v.Type())
	}
}

// TestChatStatusCodeIsSetOnFailure covers the half of requirement 7 that is
// easy to lose: the attribute must be present on the ERROR path too, or Causely
// counts the failed inference as a success.
func TestChatStatusCodeIsSetOnFailure(t *testing.T) {
	for _, tc := range []struct {
		name   string
		status int
		want   int64
	}{
		// >=500 is what increments InferenceError.
		{"server error", http.StatusInternalServerError, 500},
		// 429 is the ONLY path to the RateLimited counter, so it must survive
		// verbatim rather than being flattened to 500.
		{"rate limited", http.StatusTooManyRequests, 429},
		// A 4xx that is neither: still recorded honestly.
		{"bad request", http.StatusBadRequest, 400},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rec := setupRecorder(t)
			c := clientFor(t, openAIServer(t, tc.status), config.APIOpenAI)

			if _, err := c.Chat(context.Background(), "hi"); err == nil {
				t.Fatal("Chat succeeded, want an error")
			}

			span := rec.Ended()[0]
			if got := attrOf(t, span, "http.response.status_code").AsInt64(); got != tc.want {
				t.Errorf("http.response.status_code = %d, want %d", got, tc.want)
			}
			// The classification trigger must survive the error path, or the
			// failed span is not even recognised as genAI.
			if got := attrOf(t, span, "gen_ai.system").AsString(); got != "openai" {
				t.Errorf("gen_ai.system = %q, want openai", got)
			}
		})
	}
}

// TestChatTransportFailureReportsServerError pins the modelling choice: a
// connection failure has no status of its own, and reporting 0 would leave
// Causely counting it as a successful inference.
func TestChatTransportFailureReportsServerError(t *testing.T) {
	rec := setupRecorder(t)
	srv := openAIServer(t, http.StatusOK)
	c := clientFor(t, srv, config.APIOpenAI)
	srv.Close() // nothing is listening any more

	if _, err := c.Chat(context.Background(), "hi"); err == nil {
		t.Fatal("Chat succeeded against a closed server")
	}

	span := rec.Ended()[0]
	if got := attrOf(t, span, "http.response.status_code").AsInt64(); got != 500 {
		t.Errorf("http.response.status_code = %d, want 500 for a transport failure", got)
	}
}

// TestChatAnthropicWireFormat covers the second provider shape end to end:
// different path, different auth headers, different usage field names, same
// span contract.
func TestChatAnthropicWireFormat(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.URL.Path; got != "/v1/messages" {
			t.Errorf("path = %q, want /v1/messages", got)
		}
		if got := r.Header.Get("x-api-key"); got != "test-key" {
			t.Errorf("x-api-key = %q, want test-key", got)
		}
		if got := r.Header.Get("anthropic-version"); got == "" {
			t.Error("anthropic-version header is required by the Messages API")
		}
		// max_tokens is mandatory on this API, unlike the OpenAI one.
		var body map[string]any
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		if _, ok := body["max_tokens"]; !ok {
			t.Error("max_tokens is missing and is required")
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{
			"id": "msg_1",
			"model": "claude-haiku-4-5",
			"content": [{"type": "thinking", "text": "ignored"}, {"type": "text", "text": "two sentences."}],
			"stop_reason": "end_turn",
			"usage": {"input_tokens": 120, "output_tokens": 18}
		}`))
	}))
	t.Cleanup(srv.Close)

	rec := setupRecorder(t)
	c := clientFor(t, srv, config.APIAnthropic)

	res, err := c.Chat(context.Background(), "hi")
	if err != nil {
		t.Fatalf("Chat: %v", err)
	}
	// Only text blocks are concatenated; a thinking block must not leak in.
	if res.Text != "two sentences." {
		t.Errorf("Text = %q, want only the text block", res.Text)
	}
	if res.InputTokens != 120 || res.OutputTokens != 18 {
		t.Errorf("tokens = %d/%d, want 120/18", res.InputTokens, res.OutputTokens)
	}

	span := rec.Ended()[0]
	if got := attrOf(t, span, "gen_ai.system").AsString(); got != "anthropic" {
		t.Errorf("gen_ai.system = %q, want anthropic", got)
	}
	if got := attrOf(t, span, "gen_ai.usage.input_tokens").AsInt64(); got != 120 {
		t.Errorf("input tokens on span = %d, want 120", got)
	}
}

// TestChatSpanIsChildOfCallerSpan is requirement 8. Causely resolves the
// AIModelAccess source Operation by walking up to a registered SERVER span, and
// the collector drops INTERNAL spans — so the inference span has to attach
// directly to the caller's span rather than starting a new trace.
func TestChatSpanIsChildOfCallerSpan(t *testing.T) {
	rec := setupRecorder(t)
	c := clientFor(t, openAIServer(t, http.StatusOK), config.APIOpenAI)

	parentCtx, parent := otel.Tracer("test").Start(context.Background(), "POST /assist",
		trace.WithSpanKind(trace.SpanKindServer))
	if _, err := c.Chat(parentCtx, "hi"); err != nil {
		t.Fatalf("Chat: %v", err)
	}
	parent.End()

	var genAI sdktrace.ReadOnlySpan
	for _, s := range rec.Ended() {
		if s.SpanKind() == trace.SpanKindClient {
			genAI = s
		}
	}
	if genAI == nil {
		t.Fatal("no Client span recorded")
	}

	if got, want := genAI.Parent().SpanID(), parent.SpanContext().SpanID(); got != want {
		t.Errorf("genAI span parent = %s, want the SERVER span %s", got, want)
	}
	if got, want := genAI.SpanContext().TraceID(), parent.SpanContext().TraceID(); got != want {
		t.Errorf("genAI span trace = %s, want the caller's trace %s", got, want)
	}
}
