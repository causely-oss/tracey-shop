// Package aiassistant implements ai-assistant, the shop's genAI surface.
//
// It answers shopper questions about a product by calling a configured LLM
// provider. The point of the service existing at all, rather than storefront-bff
// making the call itself, is topological: Causely attaches an AIModelAccess to
// the *Operation* that made the inference, so a dedicated service gives the
// model access its own service.name, its own SLOs and its own place in the
// dependency chain, and lets the genAI path be scaled or broken independently of
// the shop's edge.
//
// The one structural rule here is that the provider call must happen directly
// inside this handler. The genAI CLIENT span has to be a direct child of this
// service's SERVER span for Causely to resolve the source Operation, and the
// collector drops INTERNAL spans — so introducing an intermediate span, or
// moving the call onto a background goroutine that has lost the request
// context, would leave the AIModel with no access edge and no metrics.
package aiassistant

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"strings"

	"github.com/causely-oss/tracey-shop/internal/app"
	"github.com/causely-oss/tracey-shop/internal/domain"
	"github.com/causely-oss/tracey-shop/internal/genai"
	"github.com/causely-oss/tracey-shop/internal/transport/httpx"
)

// maxQuestion bounds the prompt so token usage — and cost against a real
// provider — stays predictable regardless of what a caller sends.
const maxQuestion = 400

// Run starts the assistant.
func Run(ctx context.Context, d *app.Deps) error {
	llm := genai.New(d.Cfg, d.Faults)

	s := httpx.NewServer(d.Cfg.ServiceName, d.Cfg.HTTPAddr, d.Faults)

	s.Route("POST /assist", func(ctx context.Context, r *http.Request) (any, error) {
		var in domain.AssistRequest
		if err := httpx.DecodeJSON(r, &in); err != nil {
			return nil, err
		}

		question := strings.TrimSpace(in.Question)
		if question == "" {
			return nil, &httpx.BadRequestError{Msg: "question is required"}
		}
		if len(question) > maxQuestion {
			question = question[:maxQuestion]
		}

		res, err := llm.Chat(ctx, buildPrompt(question, in.ProductID))
		if err != nil {
			return nil, fmt.Errorf("chat completion: %w", err)
		}

		model := res.Model
		if model == "" {
			model = llm.Model()
		}
		return domain.AssistResponse{
			Answer:       res.Text,
			Model:        model,
			Provider:     llm.System(),
			InputTokens:  res.InputTokens,
			OutputTokens: res.OutputTokens,
		}, nil
	})

	slog.Info("assistant ready",
		slog.String("provider", llm.System()),
		slog.String("model", llm.Model()))

	d.Admin.SetReady(true)
	return s.Start(ctx)
}

// buildPrompt keeps the prompt short and stable.
//
// Short because every token is billed against a real provider and the demo runs
// unattended; stable because a fixed shape keeps gen_ai.usage.input_tokens in a
// narrow band, so a change in Causely's TokenInputRate means a change in
// traffic rather than noise in the prompt.
func buildPrompt(question, productID string) string {
	var b strings.Builder
	b.WriteString("You are a shop assistant. Answer in at most two sentences.\n")
	if productID != "" {
		b.WriteString("Product: ")
		b.WriteString(productID)
		b.WriteString("\n")
	}
	b.WriteString("Question: ")
	b.WriteString(question)
	return b.String()
}
