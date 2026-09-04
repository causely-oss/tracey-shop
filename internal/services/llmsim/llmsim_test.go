package llmsim

import (
	"strings"
	"testing"
)

// TestEstimateTokensVariesWithPrompt matters because Causely sums this value
// into the AIModel's TokenInput counter and derives TokenInputRate from it. A
// constant would make the token graph a flat line, and a zero would make it
// absent entirely — the mediator only records a token count above zero.
func TestEstimateTokensVariesWithPrompt(t *testing.T) {
	short := estimateTokens("hi")
	long := estimateTokens(strings.Repeat("a longer prompt ", 20))

	if short < 1 {
		t.Errorf("estimateTokens(%q) = %d, want at least 1 so the counter is recorded", "hi", short)
	}
	if long <= short {
		t.Errorf("estimateTokens did not grow with the prompt: %d then %d", short, long)
	}
	// The empty prompt is the case that would silently produce no token metric.
	if got := estimateTokens(""); got < 1 {
		t.Errorf("estimateTokens(\"\") = %d, want at least 1", got)
	}
}

func TestLastUserMessagePrefersTheLatestUserTurn(t *testing.T) {
	req := completionRequest{Messages: []message{
		{Role: "system", Content: "you are a shop assistant"},
		{Role: "user", Content: "first"},
		{Role: "assistant", Content: "an answer"},
		{Role: "user", Content: "second"},
	}}
	if got := req.lastUserMessage(); got != "second" {
		t.Errorf("lastUserMessage() = %q, want %q", got, "second")
	}

	// A system-only conversation still has to yield something rather than
	// panicking on an empty slice.
	only := completionRequest{Messages: []message{{Role: "system", Content: "just this"}}}
	if got := only.lastUserMessage(); got != "just this" {
		t.Errorf("lastUserMessage() = %q, want the fallback", got)
	}
	if got := (completionRequest{}).lastUserMessage(); got != "" {
		t.Errorf("lastUserMessage() on no messages = %q, want empty", got)
	}
}

// TestAnswerForIsOnTopic keeps the canned replies useful in a browser demo: a
// shipping question must not be answered with the returns policy.
func TestAnswerForIsOnTopic(t *testing.T) {
	for prompt, want := range map[string]string{
		"How long does delivery usually take?": "delivery",
		"What is your return policy?":          "return",
		"Does this run true to size?":          "size",
		"Is there a warranty included?":        "warranty",
	} {
		got := strings.ToLower(answerFor(prompt))
		if !strings.Contains(got, want) {
			t.Errorf("answerFor(%q) = %q, want it to mention %q", prompt, got, want)
		}
	}
	if answerFor("something entirely unrelated") == "" {
		t.Error("answerFor returned an empty default; the UI would show a blank answer")
	}
}
