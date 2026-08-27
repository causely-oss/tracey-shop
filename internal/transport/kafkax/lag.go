package kafkax

import (
	"context"
	"fmt"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/IBM/sarama"
)

// LagMonitor tracks how far a consumer group is behind on a topic.
//
// This exists so a *producer* can react to a slow consumer. Causely's causal
// model already routes consumer lag to the producing service's SLO —
// Lag.High -> Topic.Clogged -> Producers.Clogged -> Starvation ->
// producer LatencySLO/SuccessSLO — but that is only a prediction. For the
// prediction to match reality something must actually degrade, which is what
// checkout-api's order-intake backpressure does with this signal.
//
// It deliberately uses Kafka's admin API rather than scraping the metrics
// exporter: an HTTP call to the exporter would show up as a CLIENT span and
// invent a dependency edge that does not exist in the real architecture.
type LagMonitor struct {
	client sarama.Client
	admin  sarama.ClusterAdmin
	group  string
	topic  string

	lag      atomic.Int64
	observed atomic.Bool
}

// NewLagMonitor starts polling the group's lag until ctx is cancelled.
func NewLagMonitor(ctx context.Context, brokers []string, group, topic string, interval time.Duration) (*LagMonitor, error) {
	cfg := baseConfig()
	if err := waitForBroker(ctx, brokers, cfg, "lag monitor"); err != nil {
		return nil, err
	}

	client, err := sarama.NewClient(brokers, cfg)
	if err != nil {
		return nil, fmt.Errorf("lag monitor client: %w", err)
	}
	admin, err := sarama.NewClusterAdminFromClient(client)
	if err != nil {
		_ = client.Close()
		return nil, fmt.Errorf("lag monitor admin: %w", err)
	}

	m := &LagMonitor{client: client, admin: admin, group: group, topic: topic}

	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		// Sample once immediately so the first requests are not evaluated
		// against a zero that has never been measured.
		m.sample(ctx)
		for {
			select {
			case <-ctx.Done():
				// Closing the admin closes the underlying client too.
				_ = m.admin.Close()
				return
			case <-ticker.C:
				m.sample(ctx)
			}
		}
	}()

	slog.Info("consumer lag monitor started",
		slog.String("group", group),
		slog.String("topic", topic),
		slog.Duration("interval", interval))
	return m, nil
}

// Lag returns the most recently measured total lag across all partitions, and
// whether a measurement has ever succeeded. Callers must not apply backpressure
// until a measurement exists, or a monitor that cannot reach Kafka would look
// identical to a healthy consumer.
func (m *LagMonitor) Lag() (int64, bool) {
	return m.lag.Load(), m.observed.Load()
}

func (m *LagMonitor) sample(ctx context.Context) {
	if ctx.Err() != nil {
		return
	}

	// Partition membership and high-water marks move, so refresh metadata.
	if err := m.client.RefreshMetadata(m.topic); err != nil {
		slog.Debug("lag monitor: refresh metadata", slog.Any("err", err))
	}

	partitions, err := m.client.Partitions(m.topic)
	if err != nil {
		slog.Debug("lag monitor: list partitions", slog.Any("err", err))
		return
	}

	offsets, err := m.admin.ListConsumerGroupOffsets(m.group, map[string][]int32{m.topic: partitions})
	if err != nil {
		slog.Debug("lag monitor: list group offsets", slog.Any("err", err))
		return
	}

	var total int64
	for _, p := range partitions {
		newest, err := m.client.GetOffset(m.topic, p, sarama.OffsetNewest)
		if err != nil {
			slog.Debug("lag monitor: get offset", slog.Any("err", err))
			return
		}
		block := offsets.GetBlock(m.topic, p)
		// Offset -1 means the group has never committed for this partition;
		// counting newest-(-1) would report the whole log as lag.
		if block == nil || block.Offset < 0 {
			continue
		}
		if d := newest - block.Offset; d > 0 {
			total += d
		}
	}

	m.lag.Store(total)
	m.observed.Store(true)
}
