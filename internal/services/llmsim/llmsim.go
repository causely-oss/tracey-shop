// Package llmsim implements llm-sim, deployed as model-gateway.
//
// It is the demo's bundled LLM provider: an OpenAI-compatible
// /v1/chat/completions endpoint that costs nothing, needs no API key and
// reaches no network. That keeps `helm install` free of prerequisites and keeps
// the clean baseline achievable with no internet access — the same reasoning
// that produced partner-sim.
//
// Two properties matter beyond "it answers":
//
//   - It speaks the real OpenAI wire format, so the demo default exercises
//     exactly the code path in internal/genai that a real provider does, rather
//     than a mock branch that could drift.
//   - It is an ordinary role behind httpx.NewServer, so it already carries a
//     fault store. That makes the provider itself fault-injectable with no new
//     code: errorRate yields HTTP 500s, which Causely counts as InferenceError
//     and escalates to an "AIModel Malfunction" root cause, and latencyMs feeds
//     InferenceDuration_High and "AIModel Congested". No such scenario is
//     defined yet; the seam is deliberate.
package llmsim

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/causely-oss/tracey-shop/internal/app"
	"github.com/causely-oss/tracey-shop/internal/domain"
	"github.com/causely-oss/tracey-shop/internal/transport/httpx"
)

// Run starts the bundled model gateway.
func Run(ctx context.Context, d *app.Deps) error {
	latency := d.Cfg.GenAIGatewayLatency
	outputTokens := int64(d.Cfg.GenAIGatewayOutputTokens)
	if outputTokens < 1 {
		outputTokens = 1
	}

	s := httpx.NewServer(d.Cfg.ServiceName, d.Cfg.HTTPAddr, d.Faults)

	s.Route("POST /v1/chat/completions", func(ctx context.Context, r *http.Request) (any, error) {
		in, err := decodeRequest(r)
		if err != nil {
			return nil, err
		}

		// A hosted model is never instant, and a flat floor gives Causely's
		// InferenceDuration tdigest a realistic shape instead of a spike at zero.
		select {
		case <-time.After(latency):
		case <-ctx.Done():
			return nil, ctx.Err()
		}

		prompt := in.lastUserMessage()
		model := in.Model
		if model == "" {
			model = "mock-small-1"
		}

		answer := answerFor(prompt)

		return completionResponse{
			ID:      "chatcmpl-" + strings.TrimPrefix(domain.NewID("x"), "x-"),
			Object:  "chat.completion",
			Created: time.Now().Unix(),
			// A real provider returns a resolved, more specific model than the
			// one requested, which is what makes gen_ai.response.model worth
			// emitting separately.
			Model: model + "-2026-01",
			Choices: []completionChoice{{
				Index:        0,
				Message:      message{Role: "assistant", Content: answer},
				FinishReason: "stop",
			}},
			Usage: usage{
				PromptTokens:     estimateTokens(prompt),
				CompletionTokens: outputTokens,
				TotalTokens:      estimateTokens(prompt) + outputTokens,
			},
		}, nil
	})

	d.Admin.SetReady(true)
	return s.Start(ctx)
}

// ---------------------------------------------------------------------------
// Wire types
// ---------------------------------------------------------------------------

type message struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type completionRequest struct {
	Model    string    `json:"model"`
	Messages []message `json:"messages"`
}

func (r completionRequest) lastUserMessage() string {
	for i := len(r.Messages) - 1; i >= 0; i-- {
		if r.Messages[i].Role == "user" {
			return r.Messages[i].Content
		}
	}
	if len(r.Messages) > 0 {
		return r.Messages[len(r.Messages)-1].Content
	}
	return ""
}

type completionChoice struct {
	Index        int     `json:"index"`
	Message      message `json:"message"`
	FinishReason string  `json:"finish_reason"`
}

type usage struct {
	PromptTokens     int64 `json:"prompt_tokens"`
	CompletionTokens int64 `json:"completion_tokens"`
	TotalTokens      int64 `json:"total_tokens"`
}

type completionResponse struct {
	ID      string             `json:"id"`
	Object  string             `json:"object"`
	Created int64              `json:"created"`
	Model   string             `json:"model"`
	Choices []completionChoice `json:"choices"`
	Usage   usage              `json:"usage"`
}

// decodeRequest is deliberately lenient, unlike httpx.DecodeJSON.
//
// This endpoint impersonates a third-party API, and real OpenAI clients send
// plenty of fields it does not model — top_p, stream, tools, seed, and each
// provider's own extensions. Rejecting those with a 400 the way the demo's own
// strict handlers do would make the gateway fail against any client but ours,
// and a 400 here would read as an application error in Causely.
func decodeRequest(r *http.Request) (completionRequest, error) {
	var in completionRequest
	body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	if err != nil {
		return in, &httpx.BadRequestError{Msg: "read body: " + err.Error()}
	}
	if err := json.Unmarshal(body, &in); err != nil {
		return in, &httpx.BadRequestError{Msg: "invalid JSON body: " + err.Error()}
	}
	if len(in.Messages) == 0 {
		return in, &httpx.BadRequestError{Msg: "messages is required"}
	}
	return in, nil
}

// estimateTokens approximates the usual ~4-characters-per-token rule.
//
// The value only has to be plausible and to vary with the prompt: Causely sums
// it into the AIModel's TokenInput counter and derives TokenInputRate from it,
// and reads it as an integer only.
func estimateTokens(s string) int64 {
	n := int64(len(s) / 4)
	if n < 1 {
		return 1
	}
	return n
}

// answerFor returns a canned but on-topic reply, so the browser storefront shows
// something a person would believe.
func answerFor(prompt string) string {
	p := strings.ToLower(prompt)
	switch {
	case strings.Contains(p, "ship") || strings.Contains(p, "deliver"):
		return "Standard delivery arrives in 3-5 business days, and express in 1-2. " +
			"Shipping is calculated at checkout from your postcode."
	case strings.Contains(p, "return") || strings.Contains(p, "refund"):
		return "You can return anything unused within 30 days for a full refund. " +
			"Start a return from your order page and we'll email a prepaid label."
	case strings.Contains(p, "size") || strings.Contains(p, "fit"):
		return "This item runs true to size. If you're between sizes, the larger one " +
			"is the safer choice for a relaxed fit."
	case strings.Contains(p, "stock") || strings.Contains(p, "available"):
		return "Stock is shown live on the product page. If it's listed as available, " +
			"we can dispatch it today."
	case strings.Contains(p, "warrant") || strings.Contains(p, "guarantee"):
		return "It comes with a two-year manufacturer warranty covering defects, " +
			"which you can register from your order confirmation."
	default:
		return "Good question. This product is one of our better-reviewed items in its " +
			"category, and the details on this page cover the specifications, " +
			"materials and care instructions."
	}
}
