package config

import (
	"testing"
)

// TestGenAIDefaults covers normalizeGenAI, whose job is to guarantee two things
// Causely's ingest depends on:
//
//   - GENAI_SYSTEM is never empty. Its presence is the only trigger that
//     classifies a span as genAI at all, so an empty one silently reduces the
//     inference to a plain HTTP call.
//   - GENAI_MODEL is never empty and names the model actually being called. It
//     becomes half the AIModel entity name, and the mediator rejects a span with
//     no model.
func TestGenAIDefaults(t *testing.T) {
	for _, tc := range []struct {
		name        string
		env         map[string]string
		wantAPI     string
		wantModel   string
		wantSystem  string
		wantBaseURL string
	}{{
		// The shipped default: the bundled gateway, named so nobody mistakes the
		// demo for a real provider.
		name:        "bundled via explicit flag",
		env:         map[string]string{"GENAI_BUNDLED": "true", "GENAI_BASE_URL": "http://tracey-shop-model-gateway:8089"},
		wantAPI:     APIOpenAI,
		wantModel:   "mock-small-1",
		wantSystem:  "openai",
		wantBaseURL: "http://tracey-shop-model-gateway:8089",
	}, {
		// Standalone: no chart, no env at all.
		name:        "bundled inferred from an empty base URL",
		env:         map[string]string{},
		wantAPI:     APIOpenAI,
		wantModel:   "mock-small-1",
		wantSystem:  "openai",
		wantBaseURL: "http://model-gateway:8089",
	}, {
		// This is the regression the GENAI_BUNDLED flag exists for. The chart
		// always sets GENAI_BASE_URL, so an empty-URL check alone would call the
		// bundled mock "gpt-4o-mini" and make Causely report a model the demo is
		// not using.
		name:        "real openai provider",
		env:         map[string]string{"GENAI_BUNDLED": "false", "GENAI_BASE_URL": "https://api.openai.com"},
		wantAPI:     APIOpenAI,
		wantModel:   "gpt-4o-mini",
		wantSystem:  "openai",
		wantBaseURL: "https://api.openai.com",
	}, {
		name:        "real anthropic provider",
		env:         map[string]string{"GENAI_API": "anthropic", "GENAI_BUNDLED": "false", "GENAI_BASE_URL": "https://api.anthropic.com/"},
		wantAPI:     APIAnthropic,
		wantModel:   "claude-haiku-4-5",
		wantSystem:  "anthropic",
		wantBaseURL: "https://api.anthropic.com", // trailing slash trimmed
	}, {
		name: "explicit values win over every default",
		env: map[string]string{
			"GENAI_API": "OpenAI", "GENAI_BUNDLED": "false",
			"GENAI_BASE_URL": "https://api.groq.com/openai",
			"GENAI_MODEL":    "llama-3.1-8b-instant", "GENAI_SYSTEM": "groq",
		},
		wantAPI:     APIOpenAI, // case-normalised
		wantModel:   "llama-3.1-8b-instant",
		wantSystem:  "groq",
		wantBaseURL: "https://api.groq.com/openai",
	}} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("ROLE", "ai-assistant")
			for k, v := range tc.env {
				t.Setenv(k, v)
			}

			c, err := Load()
			if err != nil {
				t.Fatalf("Load: %v", err)
			}
			if c.GenAIAPI != tc.wantAPI {
				t.Errorf("GenAIAPI = %q, want %q", c.GenAIAPI, tc.wantAPI)
			}
			if c.GenAIModel != tc.wantModel {
				t.Errorf("GenAIModel = %q, want %q", c.GenAIModel, tc.wantModel)
			}
			if c.GenAISystem != tc.wantSystem {
				t.Errorf("GenAISystem = %q, want %q", c.GenAISystem, tc.wantSystem)
			}
			if c.GenAIBaseURL != tc.wantBaseURL {
				t.Errorf("GenAIBaseURL = %q, want %q", c.GenAIBaseURL, tc.wantBaseURL)
			}

			// The two invariants, restated as assertions so a future default
			// cannot quietly blank either one.
			if c.GenAISystem == "" {
				t.Error("GenAISystem is empty; the span would not be classified as genAI")
			}
			if c.GenAIModel == "" {
				t.Error("GenAIModel is empty; the mediator rejects a span with no model")
			}
		})
	}
}

// TestGenAIStandaloneDefaultsAreInert covers the bare-binary case only.
//
// The chart is what enables genAI (genai.enabled: true, loadgen.assistRPS: 0.5),
// so that the AI entities exist in Causely from the first install. Running the
// binary standalone has no model-gateway to answer, so both stay off here —
// otherwise `go run ./cmd/shopd` for local debugging would emit a stream of
// failing inferences.
func TestGenAIStandaloneDefaultsAreInert(t *testing.T) {
	t.Setenv("ROLE", "loadgen")
	c, err := Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if c.LoadAssistRPS != 0 {
		t.Errorf("LoadAssistRPS = %v, want 0 without the chart", c.LoadAssistRPS)
	}
	if c.GenAIEnabled {
		t.Error("GenAIEnabled = true without the chart; only the chart should enable it")
	}
}

// TestGenAIRateClearsTheSymptomActivationGate pins the one number that decides
// whether the genAI scenarios can produce anything at all.
//
// Every genAI symptom in Causely — Inference Latency, Inference Error Rate and
// Rate Limited — is gated on `InferenceTotalRate > 0.1`. Below that the AIModel
// entity and its token metrics still appear, so the demo looks fine, but no
// symptom can ever fire and ai-model-malfunction silently produces nothing.
func TestGenAIRateClearsTheSymptomActivationGate(t *testing.T) {
	const activationGate = 0.1

	t.Setenv("ROLE", "loadgen")
	// What deploy/tracey-shop/values.yaml ships.
	t.Setenv("LOAD_ASSIST_RPS", "0.5")

	c, err := Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if c.LoadAssistRPS <= activationGate {
		t.Errorf("LoadAssistRPS = %v, which is at or below Causely's InferenceTotalRate > %v "+
			"activation gate; no genAI symptom could ever fire", c.LoadAssistRPS, activationGate)
	}
}
