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
