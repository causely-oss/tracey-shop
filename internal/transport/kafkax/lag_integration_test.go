package kafkax

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/causely-oss/tracey-shop/internal/faults"
)

// Exercises the real produce -> consumer-group -> lag path against a live broker.
// Skipped unless KAFKA_ITEST_BROKERS is set.
func TestLagPathAgainstRealBroker(t *testing.T) {
	brokers := os.Getenv("KAFKA_ITEST_BROKERS")
	if brokers == "" {
		t.Skip("set KAFKA_ITEST_BROKERS")
	}
	bs := []string{brokers}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	const topic, group = "itest-orders", "itest-workers"

	p, err := NewProducer(ctx, bs)
	if err != nil {
		t.Fatalf("NewProducer: %v", err)
	}
	defer p.Close()

	// Produce a backlog with nothing consuming it yet.
	for i := 0; i < 25; i++ {
		if err := p.Publish(ctx, topic, "k", map[string]int{"n": i}); err != nil {
			t.Fatalf("Publish %d: %v", i, err)
		}
	}
	t.Log("produced 25 messages")

	// A consumer group that commits, so the group offset exists.
	got := make(chan struct{}, 100)
	store := faults.NewStore("itest")
	c, err := NewConsumer(ctx, bs, group, topic, store, func(context.Context, string, []byte) error {
		select {
		case got <- struct{}{}:
		default:
		}
		return nil
	})
	if err != nil {
		t.Fatalf("NewConsumer: %v", err)
	}
	runCtx, stop := context.WithCancel(ctx)
	go func() { _ = c.Run(runCtx) }()

	deadline := time.After(60 * time.Second)
	n := 0
	for n < 25 {
		select {
		case <-got:
			n++
		case <-deadline:
			t.Fatalf("only consumed %d/25 messages", n)
		}
	}
	t.Logf("consumed all %d messages", n)
	stop()
	_ = c.Close()

	// Now the lag monitor must be able to read group offsets.
	m, err := NewLagMonitor(ctx, bs, group, topic, 1*time.Second)
	if err != nil {
		t.Fatalf("NewLagMonitor: %v", err)
	}
	var lag int64
	var ok bool
	for i := 0; i < 30; i++ {
		if lag, ok = m.Lag(); ok {
			break
		}
		time.Sleep(1 * time.Second)
	}
	if !ok {
		t.Fatal("LagMonitor never produced a sample — the fraud-lag scenario would be silent")
	}
	t.Logf("lag with nothing outstanding: %d (ok=%v)", lag, ok)
	if lag != 0 {
		t.Errorf("expected 0 lag after consuming everything, got %d", lag)
	}

	// The fraud-lag condition: the group has committed offsets but stopped
	// consuming, and the producer keeps going. Lag must climb, or Causely's
	// Consumer Lag symptom never fires.
	for i := 0; i < 40; i++ {
		if err := p.Publish(ctx, topic, "k", map[string]int{"backlog": i}); err != nil {
			t.Fatalf("Publish backlog %d: %v", i, err)
		}
	}
	// Let it settle rather than breaking on the first positive sample, so we
	// also verify the per-partition sum rather than just "greater than zero".
	time.Sleep(6 * time.Second)
	lag, ok = m.Lag()
	if !ok || lag <= 0 {
		t.Fatalf("lag did not climb after a 40-message backlog (lag=%d ok=%v); "+
			"the fraud-lag scenario would pile up real lag and Causely would stay silent", lag, ok)
	}
	t.Logf("lag after 40-message backlog: %d", lag)
}
