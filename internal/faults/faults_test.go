package faults

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestNewStoreIsClean(t *testing.T) {
	s := NewStore("payment-gw")
	if !s.Get().IsZero() {
		t.Fatalf("a fresh store must have no faults, got %+v", s.Get())
	}
	// The clean baseline depends on Gate being a no-op by default.
	if err := s.Gate(context.Background()); err != nil {
		t.Fatalf("Gate on a clean store returned %v", err)
	}
}

func TestApplyMergesPartialPatch(t *testing.T) {
	s := NewStore("payment-gw")

	rate := 0.5
	s.Apply(Patch{ErrorRate: &rate})
	if got := s.Get().ErrorRate; got != 0.5 {
		t.Fatalf("ErrorRate = %v, want 0.5", got)
	}

	// A second patch must not reset fields it does not mention.
	latency := 250
	s.Apply(Patch{LatencyMs: &latency})
	spec := s.Get()
	if spec.ErrorRate != 0.5 {
		t.Errorf("ErrorRate was clobbered by an unrelated patch: %v", spec.ErrorRate)
	}
	if spec.LatencyMs != 250 {
		t.Errorf("LatencyMs = %v, want 250", spec.LatencyMs)
	}
}

func TestErrorRateOneAlwaysFails(t *testing.T) {
	s := NewStore("payment-gw")
	rate := 1.0
	s.Apply(Patch{ErrorRate: &rate})

	for i := 0; i < 20; i++ {
		err := s.Gate(context.Background())
		if !errors.Is(err, ErrInjected) {
			t.Fatalf("iteration %d: Gate returned %v, want ErrInjected", i, err)
		}
	}
}

func TestClearRestoresHealthAndReleasesLeaks(t *testing.T) {
	s := NewStore("payment-gw")

	rate := 1.0
	leak := 8
	dbLeak := true
	s.Apply(Patch{ErrorRate: &rate, MemLeakKBPerReq: &leak, DBConnLeak: &dbLeak})

	// Accumulate some ballast, and register a leaked connection.
	_ = s.Gate(context.Background())
	released := false
	if !s.LeakConn(func() { released = true }) {
		t.Fatal("LeakConn should retain the release func while dbConnLeak is on")
	}

	s.Clear()

	if !s.Get().IsZero() {
		t.Errorf("Clear left faults active: %+v", s.Get())
	}
	// Recovery without a restart is what makes scenarios repeatable in a demo.
	if err := s.Gate(context.Background()); err != nil {
		t.Errorf("Gate after Clear returned %v, want nil", err)
	}
	if !released {
		t.Error("Clear did not release the leaked connection")
	}
	if len(s.ballast) != 0 {
		t.Errorf("Clear did not drop the memory ballast (%d chunks retained)", len(s.ballast))
	}
}

func TestLeakConnIsInertWhenFaultIsOff(t *testing.T) {
	s := NewStore("payment-gw")
	if s.LeakConn(func() {}) {
		t.Fatal("LeakConn must return false when dbConnLeak is off, so callers release normally")
	}
}

func TestLatencyIsApplied(t *testing.T) {
	s := NewStore("payment-gw")
	latency := 60
	s.Apply(Patch{LatencyMs: &latency})

	start := time.Now()
	if err := s.Gate(context.Background()); err != nil {
		t.Fatalf("Gate returned %v", err)
	}
	if elapsed := time.Since(start); elapsed < 50*time.Millisecond {
		t.Errorf("Gate returned after %v, expected at least ~60ms of injected latency", elapsed)
	}
}

func TestGateHonoursContextCancellation(t *testing.T) {
	s := NewStore("payment-gw")
	latency := 5000
	s.Apply(Patch{LatencyMs: &latency})

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	start := time.Now()
	err := s.Gate(ctx)
	if err == nil {
		t.Fatal("Gate should return the context error rather than sleeping through cancellation")
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Errorf("Gate slept %v despite a cancelled context", elapsed)
	}
}

func TestClientTimeoutOverride(t *testing.T) {
	s := NewStore("payment-gw")
	def := 5 * time.Second

	if got := s.ClientTimeout(def); got != def {
		t.Errorf("ClientTimeout = %v, want the default %v when no fault is set", got, def)
	}

	ms := 500
	s.Apply(Patch{DependencyTimeoutMs: &ms})
	if got := s.ClientTimeout(def); got != 500*time.Millisecond {
		t.Errorf("ClientTimeout = %v, want 500ms", got)
	}
}

func TestDecodePatchIgnoresEmptyBody(t *testing.T) {
	p, err := DecodePatch(nil)
	if err != nil {
		t.Fatalf("DecodePatch(nil) returned %v", err)
	}
	if p.ErrorRate != nil || p.LatencyMs != nil {
		t.Errorf("an empty body must produce an all-nil patch, got %+v", p)
	}
}

func TestDecodePatchRoundTrip(t *testing.T) {
	p, err := DecodePatch([]byte(`{"errorRate":0.35,"consumerStall":true}`))
	if err != nil {
		t.Fatalf("DecodePatch returned %v", err)
	}
	if p.ErrorRate == nil || *p.ErrorRate != 0.35 {
		t.Errorf("ErrorRate = %v, want 0.35", p.ErrorRate)
	}
	if p.ConsumerStall == nil || !*p.ConsumerStall {
		t.Errorf("ConsumerStall = %v, want true", p.ConsumerStall)
	}
	if p.LatencyMs != nil {
		t.Errorf("LatencyMs should stay nil when absent, got %v", *p.LatencyMs)
	}
}
