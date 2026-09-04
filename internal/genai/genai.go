// Package genai is the demo's LLM provider client.
//
// It exists to emit one GenAI OpenTelemetry span per inference, shaped so
// Causely's mediator actually interprets it. That contract is strict and every
// way of failing it is SILENT — the mediator logs at Debug and moves on — so
// each requirement is called out where it is satisfied. Derived from
// pkg/spananalyzer/{semconv.go,span_analyzer.go}, cmd/mediator/otlp/
// trace_client_gen_ai.go and pkg/model/operation_genai.go in the causely repo:
//
//  1. The span MUST be CLIENT. GenAI is only analysed in the client pass;
//     INTERNAL spans are ignored (and dropped at our collector anyway).
//  2. It MUST carry gen_ai.system (or gen_ai.provider.name / llm.provider).
//     Presence alone is the ONLY trigger that classifies a span as genAI, and it
//     is checked before HTTP, RPC and database. The value is never validated —
//     but without one the span is just an HTTP call and no AIModel is created.
//  3. It MUST carry server.address, or analyzeGenAISpan returns immediately.
//     server.port defaults to 443 when absent.
//  4. gen_ai.request.model (else gen_ai.response.model) MUST be non-empty;
//     CreateAIModel rejects an empty model name. The AIModel entity is named
//     "<model>/<operation>", e.g. "gpt-4o-mini/chat".
//  5. gen_ai.operation.name is taken verbatim into that name and is never
//     switched on. It defaults to "chat" if missing.
//  6. Token counts MUST be integer-typed. The mediator's attribute reader
//     accepts IntValue only, so a float or string is silently read as 0.
//  7. http.response.status_code MUST be present. It is the ONLY source of error
//     and rate-limit signal: >=500 increments InferenceError, ==429 increments
//     RateLimited. The OTLP span status.code is ignored entirely — so
//     span.SetStatus, which every other service in this demo relies on, buys
//     nothing here.
//  8. The span MUST be a direct child of the caller's SERVER span, which
//     supplies the AIModelAccess source Operation. See the note on the tracer
//     below.
//
// Anything not in that list is discarded on ingest (the mediator keeps only
// mapped keys), but is still emitted where the OTel spec calls for it so the
// traces remain correct for other backends.
package genai

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	semconv "go.opentelemetry.io/otel/semconv/v1.34.0"
	"go.opentelemetry.io/otel/trace"

	"github.com/causely-oss/tracey-shop/internal/config"
	"github.com/causely-oss/tracey-shop/internal/faults"
	"github.com/causely-oss/tracey-shop/internal/obs"
	"github.com/causely-oss/tracey-shop/internal/transport/httpx"
)

// providerNameKey is gen_ai.provider.name, which replaced gen_ai.system in
// semconv 1.37. There is no constant for it in the v1.34.0 package this repo
// pins (and it is absent again from the newer v1.43.0 package, where the
// experimental gen_ai group was dropped), so it is spelled out. Causely accepts
// either, and emitting both costs one attribute and keeps the traces correct for
// a newer collector.
const providerNameKey = attribute.Key("gen_ai.provider.name")

// operationChat is the only operation the demo performs today. It is a plain
// string on purpose: Causely copies it into the entity name verbatim, so
// widening this later needs no ingest-side change.
const operationChat = "chat"

// Result is one completed inference.
type Result struct {
	Text         string
	Model        string
	InputTokens  int64
	OutputTokens int64
	FinishReason string
}

// Client talks to one configured provider.
type Client struct {
	api         string
	model       string
	system      string
	maxTokens   int
	temperature float64
	http        *httpx.Client
	tracer      trace.Tracer
}

// New builds the client described by the environment.
func New(cfg *config.Config, store *faults.Store) *Client {
	opts := []httpx.ClientOption{
		// Requirement 1 and 8: we create the CLIENT span ourselves so it can
		// carry the gen_ai.* attributes and sit directly under the caller's
		// SERVER span. Letting otelhttp wrap the call as well would produce a
		// second CLIENT span to the same peer.
		httpx.WithCallerSpan(),
	}

	switch cfg.GenAIAPI {
	case config.APIAnthropic:
		if cfg.GenAIAPIKey != "" {
			opts = append(opts,
				httpx.WithHeader("x-api-key", cfg.GenAIAPIKey),
				httpx.WithHeader("anthropic-version", "2023-06-01"))
		}
	default:
		// The bundled model-gateway needs no credential, which is what keeps
		// `helm install` free of prerequisites.
		if cfg.GenAIAPIKey != "" {
			opts = append(opts, httpx.WithHeader("Authorization", "Bearer "+cfg.GenAIAPIKey))
		}
	}

	return &Client{
		api:         cfg.GenAIAPI,
		model:       cfg.GenAIModel,
		system:      cfg.GenAISystem,
		maxTokens:   cfg.GenAIMaxTokens,
		temperature: cfg.GenAITemperature,
		http:        httpx.NewClient(cfg.GenAIBaseURL, cfg.GenAITimeout, store, opts...),
		tracer:      obs.Tracer("genai"),
	}
}

// Model is the configured request model, as it appears in gen_ai.request.model.
func (c *Client) Model() string { return c.model }

// System is the configured provider, as it appears in gen_ai.system.
func (c *Client) System() string { return c.system }

// Chat performs one chat completion.
//
// ctx MUST carry the caller's SERVER span. Requirement 8: Causely resolves the
// AIModelAccess source Operation by walking up to a registered SERVER span, and
// our collector drops INTERNAL spans — so an intermediate INTERNAL span here
// would orphan the parent chain and leave the AIModel with no access edge and no
// metrics at all. Do not wrap this call in one.
func (c *Client) Chat(ctx context.Context, prompt string) (Result, error) {
	// Requirement 5: the span name follows the semconv "<operation> <model>"
	// convention. Causely never parses it, but every other backend shows it.
	ctx, span := c.tracer.Start(ctx, operationChat+" "+c.model,
		trace.WithSpanKind(trace.SpanKindClient), // requirement 1
		trace.WithAttributes(
			semconv.GenAIOperationNameChat,             // requirement 5
			semconv.GenAISystemKey.String(c.system),    // requirement 2
			providerNameKey.String(c.system),           // requirement 2, current spelling
			semconv.GenAIRequestModel(c.model),         // requirement 4
			semconv.ServerAddress(c.http.Host()),       // requirement 3
			semconv.ServerPort(c.http.Port()),          // requirement 3
			semconv.GenAIRequestMaxTokens(c.maxTokens), // dropped on ingest; correct OTel
			semconv.GenAIRequestTemperature(c.temperature),
		))
	defer span.End()

	res, status, err := c.invoke(ctx, prompt)

	// Requirement 7. Set unconditionally, on both paths: this attribute is the
	// only thing that can ever mark an inference as failed or rate-limited, and
	// a span without it is counted as a success no matter what went wrong.
	span.SetAttributes(semconv.HTTPResponseStatusCode(status))

	if err != nil {
		// span.SetStatus is for every other consumer of these traces; Causely
		// reads the attribute above instead.
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return Result{}, err
	}

	span.SetAttributes(
		semconv.GenAIUsageInputTokens(int(res.InputTokens)),   // requirement 6
		semconv.GenAIUsageOutputTokens(int(res.OutputTokens)), // requirement 6
	)
	if res.Model != "" {
		span.SetAttributes(semconv.GenAIResponseModel(res.Model)) // requirement 4 fallback
	}
	if res.FinishReason != "" {
		span.SetAttributes(semconv.GenAIResponseFinishReasons(res.FinishReason))
	}
	return res, nil
}

// invoke dispatches to the configured wire format and always returns an HTTP
// status code to put on the span.
//
// A transport failure — connection refused, or the dependencyTimeoutMs fault
// biting — carries no status of its own, so it is reported as 500. That is a
// deliberate modelling choice, not a fudge: a failed inference IS an inference
// error, and 500 is the only value that makes Causely's InferenceErrorRate see
// it (requirement 7). Reporting 0 would silently count the failure as a success.
func (c *Client) invoke(ctx context.Context, prompt string) (Result, int, error) {
	var (
		res Result
		err error
	)
	switch c.api {
	case config.APIAnthropic:
		res, err = c.chatAnthropic(ctx, prompt)
	case config.APIOpenAI:
		res, err = c.chatOpenAI(ctx, prompt)
	default:
		return Result{}, http.StatusInternalServerError,
			fmt.Errorf("unsupported GENAI_API %q: want %q or %q", c.api, config.APIOpenAI, config.APIAnthropic)
	}
	if err != nil {
		return Result{}, statusFromError(err), err
	}
	return res, http.StatusOK, nil
}

// statusFromError recovers the provider's own status code when there was one.
func statusFromError(err error) int {
	var se *httpx.StatusError
	if errors.As(err, &se) && se.Status > 0 {
		// Preserves a real 429, which is the only path to RateLimited.
		return se.Status
	}
	return http.StatusInternalServerError
}

// ---------------------------------------------------------------------------
// OpenAI-compatible wire format
// ---------------------------------------------------------------------------

// This is the format nearly every provider and self-hosted server speaks:
// OpenAI, Groq, Together, OpenRouter, Fireworks, DeepSeek, xAI, Mistral,
// Ollama, vLLM, LiteLLM, Azure OpenAI and Gemini's compatibility endpoint. The
// bundled model-gateway speaks it too, so the demo default exercises exactly the
// code path a real provider does.

type openAIMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type openAIRequest struct {
	Model    string          `json:"model"`
	Messages []openAIMessage `json:"messages"`
	// max_tokens rather than max_completion_tokens: the newer spelling is
	// OpenAI-only, while every compatible server still accepts this one.
	MaxTokens   int     `json:"max_tokens,omitempty"`
	Temperature float64 `json:"temperature,omitempty"`
}

type openAIResponse struct {
	ID      string `json:"id"`
	Model   string `json:"model"`
	Choices []struct {
		Message      openAIMessage `json:"message"`
		FinishReason string        `json:"finish_reason"`
	} `json:"choices"`
	Usage struct {
		PromptTokens     int64 `json:"prompt_tokens"`
		CompletionTokens int64 `json:"completion_tokens"`
	} `json:"usage"`
}

func (c *Client) chatOpenAI(ctx context.Context, prompt string) (Result, error) {
	var out openAIResponse
	err := c.http.PostJSON(ctx, "/v1/chat/completions", openAIRequest{
		Model:       c.model,
		Messages:    []openAIMessage{{Role: "user", Content: prompt}},
		MaxTokens:   c.maxTokens,
		Temperature: c.temperature,
	}, &out)
	if err != nil {
		return Result{}, err
	}

	res := Result{
		Model:        out.Model,
		InputTokens:  out.Usage.PromptTokens,
		OutputTokens: out.Usage.CompletionTokens,
	}
	if len(out.Choices) > 0 {
		res.Text = strings.TrimSpace(out.Choices[0].Message.Content)
		res.FinishReason = out.Choices[0].FinishReason
	}
	return res, nil
}

// ---------------------------------------------------------------------------
// Anthropic Messages wire format
// ---------------------------------------------------------------------------

type anthropicRequest struct {
	Model       string          `json:"model"`
	Messages    []openAIMessage `json:"messages"`
	MaxTokens   int             `json:"max_tokens"`
	Temperature float64         `json:"temperature,omitempty"`
}

type anthropicResponse struct {
	ID      string `json:"id"`
	Model   string `json:"model"`
	Content []struct {
		Type string `json:"type"`
		Text string `json:"text"`
	} `json:"content"`
	StopReason string `json:"stop_reason"`
	Usage      struct {
		InputTokens  int64 `json:"input_tokens"`
		OutputTokens int64 `json:"output_tokens"`
	} `json:"usage"`
}

func (c *Client) chatAnthropic(ctx context.Context, prompt string) (Result, error) {
	var out anthropicResponse
	// max_tokens is required by this API, unlike the OpenAI one.
	maxTokens := c.maxTokens
	if maxTokens <= 0 {
		maxTokens = 256
	}
	err := c.http.PostJSON(ctx, "/v1/messages", anthropicRequest{
		Model:       c.model,
		Messages:    []openAIMessage{{Role: "user", Content: prompt}},
		MaxTokens:   maxTokens,
		Temperature: c.temperature,
	}, &out)
	if err != nil {
		return Result{}, err
	}

	res := Result{
		Model:        out.Model,
		InputTokens:  out.Usage.InputTokens,
		OutputTokens: out.Usage.OutputTokens,
		FinishReason: out.StopReason,
	}
	// The response is a list of content blocks; concatenate the text ones.
	var b strings.Builder
	for _, blk := range out.Content {
		if blk.Type == "text" {
			b.WriteString(blk.Text)
		}
	}
	res.Text = strings.TrimSpace(b.String())
	return res, nil
}
