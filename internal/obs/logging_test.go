package obs

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// Causely promotes WARN and ERROR container logs into root-cause evidence, and
// its description generator will build a narrative around whatever it finds in a
// defect's closure. Control-plane chatter at those levels therefore does real
// damage: it produces confident, wrong root causes.
//
// This has now happened twice:
//
//   - "fault spec updated" at Warn → root cause "Fault spec updated causing
//     payment authorization malfunction", remediation "revert the fault
//     specification update". It exposed the injection mechanism.
//   - "load configuration changed" at Warn (a single line, count 1, emitted by
//     the traffic generator's admin API) → a *Critical* "Service Malfunction"
//     on checkout-api blaming "load configuration changes and deployment
//     settings", which outranked the real 35% payment-gw failure.
//
// So the rule is enforced rather than remembered: anything describing an
// operator action on the admin API logs at Debug or Info, never Warn or Error.
var controlPlanePhrases = []string{
	"fault spec",
	"load configuration",
	"faults cleared",
	"scenario",
}

// logCall matches a slog.Warn( or slog.Error( call and captures the level plus
// the start of the message literal.
var logCall = regexp.MustCompile(`slog\.(Warn|Error)\(\s*"([^"]*)"`)

func TestControlPlaneEventsAreNotLoggedAtWarnOrError(t *testing.T) {
	root := filepath.Join("..", "..")

	err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			// Generated protobuf code and vendored trees are not ours.
			if name := info.Name(); name == "gen" || name == ".git" || name == "deploy" {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}

		src, readErr := os.ReadFile(path)
		if readErr != nil {
			return readErr
		}

		for _, m := range logCall.FindAllStringSubmatch(string(src), -1) {
			level, msg := m[1], strings.ToLower(m[2])
			for _, phrase := range controlPlanePhrases {
				if strings.Contains(msg, phrase) {
					t.Errorf("%s: slog.%s(%q) — control-plane events must log at Debug or Info, "+
						"or Causely turns them into root-cause evidence and invents a defect around them",
						path, level, m[2])
				}
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk: %v", err)
	}
}

// TestRenderedEnvVarNamesDoNotRevealTheDemo extends the narrative rule from log
// messages to environment variable names.
//
// Env var names are part of the pod spec, which Causely reads — and quotes back
// in its remediation advice. Observed verbatim in an `AIModel Malfunction`
// remediation on a live cluster:
//
//	"consider adjusting the GENAI_SIM_LATENCY and REQUEST_TIMEOUT parameters"
//
// A root cause whose recommended fix names a knob called SIM tells the audience
// the incident was staged, exactly like a log line naming the injection. The
// variable was renamed to GENAI_GATEWAY_LATENCY; this keeps it that way.
//
// Deliberately scoped to the chart's env var NAMES, not values, and not the
// ROLE values (`llm-sim`, `partner-sim`) which predate this rule and are
// accepted — the demo already presents `stripe-sim` and friends as service
// names in the topology.
func TestRenderedEnvVarNamesDoNotRevealTheDemo(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("..", "..", "deploy", "tracey-shop", "templates", "_helpers.tpl"))
	if err != nil {
		t.Fatalf("read _helpers.tpl: %v", err)
	}

	// Same vocabulary internal/faults/narrative_test.go forbids in messages.
	banned := []string{
		"_SIM_", "_SIM ", "SIM_", "FAULT", "INJECT", "SCENARIO",
		"SYNTHETIC", "CHAOS", "DEMO_", "MOCK", "FAKE", "STUB",
	}

	// Match `- name: SOME_VAR` in the env blocks.
	re := regexp.MustCompile(`(?m)^\s*-\s+name:\s+([A-Z][A-Z0-9_]*)\s*$`)
	matches := re.FindAllStringSubmatch(string(raw), -1)
	if len(matches) == 0 {
		t.Fatal("found no env var names in _helpers.tpl; the matcher is broken, " +
			"so a pass here would be meaningless")
	}

	for _, m := range matches {
		name := m[1]
		for _, bad := range banned {
			if strings.Contains(name, bad) {
				t.Errorf("env var %q contains %q — it appears in the pod spec, and Causely "+
					"quotes pod-spec settings in its remediation advice, which would reveal "+
					"that the incident was staged", name, bad)
			}
		}
	}
	t.Logf("checked %d env var names", len(matches))
}
