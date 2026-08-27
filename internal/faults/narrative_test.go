package faults

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"
)

// forbidden are words that would give away that the incident was staged.
//
// This is the guardrail for the whole narrative idea. Causely synthesises its
// root-cause description from WARN/ERROR log lines, so any of these leaking into
// a message ends up in the customer-facing RCA. A real regression: an earlier
// version logged "fault spec updated" at WARN, and Causely reported the root
// cause as "Fault spec updated causing payment authorization malfunction" with
// remediation "revert the fault specification update".
var forbidden = []string{
	"fault", "inject", "injected", "scenario", "simulat", "synthetic",
	"demo", "chaos", "artificial", "deliberate", "test harness",
}

func TestNarrativesNeverRevealTheInjection(t *testing.T) {
	check := func(t *testing.T, service, field, msg string) {
		t.Helper()
		if msg == "" {
			t.Errorf("%s.%s is empty; emit would log a blank message", service, field)
			return
		}
		lower := strings.ToLower(msg)
		for _, bad := range forbidden {
			if strings.Contains(lower, bad) {
				t.Errorf("%s.%s contains %q, which would expose the injection in Causely's RCA: %q",
					service, field, bad, msg)
			}
		}
	}

	// Every field of every narrative, including the default set.
	all := map[string]Narrative{"<default>": defaultNarrative}
	for svc, n := range narratives {
		all[svc] = n
	}

	for service, n := range all {
		v := reflect.ValueOf(n)
		typ := v.Type()
		for i := 0; i < v.NumField(); i++ {
			// Blank fields in a per-service narrative are filled from the
			// default set by narrativeFor, so only check what is set.
			msg := v.Field(i).String()
			if msg == "" && service != "<default>" {
				continue
			}
			check(t, service, typ.Field(i).Name, msg)
		}
	}
}

// TestNarrativeForFillsEveryField guarantees no code path can log an empty
// message, however sparse a service's narrative is.
func TestNarrativeForFillsEveryField(t *testing.T) {
	for _, service := range []string{"payment-gw", "risk-model", "cart-service", "does-not-exist"} {
		n := narrativeFor(service)
		v := reflect.ValueOf(n)
		typ := v.Type()
		for i := 0; i < v.NumField(); i++ {
			if v.Field(i).String() == "" {
				t.Errorf("narrativeFor(%q).%s is empty", service, typ.Field(i).Name)
			}
		}
	}
}

// TestNarrativesAreServiceSpecific checks the messages actually differ per
// service — a table where everything fell back to the default would produce
// identical, useless evidence for every scenario.
func TestNarrativesAreServiceSpecific(t *testing.T) {
	cases := map[string]struct {
		field string
		want  string
	}{
		"payment-gw":     {"Error", "authorization"},
		"ledger-svc":     {"SlowQuery", "journal"},
		"inventory-svc":  {"Memory", "stock"},
		"fraud-detector": {"ConsumerStall", "offsets"},
		"pricing-engine": {"CPU", "rule"},
		"catalog-api":    {"CacheBypass", "cache"},
		"risk-model":     {"Panic", "feature vector"},
		"checkout-api":   {"DependencyTimeout", "deadline"},
		"cart-service":   {"Latency", "cart"},
	}

	for service, tc := range cases {
		n := narrativeFor(service)
		got := reflect.ValueOf(n).FieldByName(tc.field).String()
		if !strings.Contains(strings.ToLower(got), tc.want) {
			t.Errorf("narrativeFor(%q).%s = %q, expected it to mention %q",
				service, tc.field, got, tc.want)
		}
	}
}

// TestLogLimiterThrottlesAndCounts covers the rate limiter. Without it,
// pricing-cpu on the browse path would emit tens of lines per second per pod.
func TestLogLimiterThrottlesAndCounts(t *testing.T) {
	l := newLogLimiter()

	// First call always passes, with nothing suppressed yet.
	ok, dropped := l.allow("msg")
	if !ok || dropped != 0 {
		t.Fatalf("first allow = (%v, %d), want (true, 0)", ok, dropped)
	}

	// Subsequent calls inside the window are suppressed and counted.
	for i := 0; i < 5; i++ {
		if ok, _ := l.allow("msg"); ok {
			t.Fatalf("call %d passed inside the %s window", i, logInterval)
		}
	}

	// A different message has its own budget.
	if ok, _ := l.allow("other"); !ok {
		t.Error("a distinct message should not be throttled by another's window")
	}

	// Force the window open and confirm the suppressed count is reported once,
	// then reset.
	l.mu.Lock()
	l.last["msg"] = time.Now().Add(-2 * logInterval)
	l.mu.Unlock()

	ok, dropped = l.allow("msg")
	if !ok {
		t.Fatal("allow should pass once the window has elapsed")
	}
	if dropped != 5 {
		t.Errorf("suppressed count = %d, want 5", dropped)
	}

	l.mu.Lock()
	l.last["msg"] = time.Now().Add(-2 * logInterval)
	l.mu.Unlock()
	if _, dropped = l.allow("msg"); dropped != 0 {
		t.Errorf("suppressed count = %d after reporting, want 0", dropped)
	}
}

// TestCleanStoreLogsNothing protects the baseline: with no fault active, Gate
// must not emit anything Causely could read as evidence.
func TestCleanStoreLogsNothing(t *testing.T) {
	s := NewStore("payment-gw")
	if !s.Get().IsZero() {
		t.Fatal("a fresh store should have no faults")
	}
	// Gate returns before touching the limiter when the spec is zero.
	if err := s.Gate(nil); err != nil { //nolint:staticcheck // exercising the zero-spec fast path
		t.Fatalf("Gate on a clean store returned %v", err)
	}
	s.limiter.mu.Lock()
	emitted := len(s.limiter.last)
	s.limiter.mu.Unlock()
	if emitted != 0 {
		t.Errorf("a clean store emitted %d log message(s); the baseline must stay silent", emitted)
	}
}

// TestInjectedErrorTextNeverRevealsTheInjection covers the path the narrative
// tests do not: the error VALUE, not the log line.
//
// gRPC error strings propagate up the whole call chain and end up in the JSON
// body storefront-bff returns to the browser, which the storefront renders on
// screen. A sentinel reading "injected fault" put those words in front of a
// customer during a demo.
func TestInjectedErrorTextNeverRevealsTheInjection(t *testing.T) {
	s := NewStore("payment-gw")
	rate := 1.0
	s.Apply(Patch{ErrorRate: &rate})

	err := s.Gate(context.Background())
	if err == nil {
		t.Fatal("expected the errorRate fault to fail the request")
	}

	// Callers still have to be able to identify it.
	if !errors.Is(err, ErrInjected) {
		t.Fatalf("errors.Is(err, ErrInjected) is false for %v; callers cannot classify it", err)
	}

	lower := strings.ToLower(err.Error())
	for _, bad := range forbidden {
		if strings.Contains(lower, bad) {
			t.Errorf("the error returned to callers contains %q and reaches the browser: %q",
				bad, err.Error())
		}
	}

	// It should be the service's narrative message, so the UI and any log read
	// like a real payment failure.
	if err.Error() != narrativeFor("payment-gw").Error {
		t.Errorf("error text = %q, want the service narrative %q",
			err.Error(), narrativeFor("payment-gw").Error)
	}

	// ErrInjected's own text is a fallback that should also be safe.
	for _, bad := range forbidden {
		if strings.Contains(strings.ToLower(ErrInjected.Error()), bad) {
			t.Errorf("ErrInjected's text contains %q: %q", bad, ErrInjected.Error())
		}
	}
}
