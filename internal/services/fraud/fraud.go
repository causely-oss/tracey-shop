// Package fraud implements fraud-detector: a Kafka consumer that scores each
// order via risk-model and republishes a notification.
//
// It is the demo's only service with no inbound synchronous traffic, so its
// health cannot be inferred from an upstream caller's error rate. The
// consumerStall fault therefore produces consumer lag on the orders topic and
// nothing else — a failure shape that only a topology-aware system can attribute.
package fraud

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	shopv1 "github.com/causely-oss/tracey-shop/gen/shop/v1"
	"github.com/causely-oss/tracey-shop/internal/app"
	"github.com/causely-oss/tracey-shop/internal/domain"
	"github.com/causely-oss/tracey-shop/internal/obs"
	"github.com/causely-oss/tracey-shop/internal/transport/grpcx"
	"github.com/causely-oss/tracey-shop/internal/transport/kafkax"
)

// Run starts the fraud-detector consumer loop.
func Run(ctx context.Context, d *app.Deps) error {
	risk, err := d.RiskClient()
	if err != nil {
		return fmt.Errorf("risk client: %w", err)
	}
	producer, err := d.Producer(ctx)
	if err != nil {
		return fmt.Errorf("kafka producer: %w", err)
	}

	handler := func(ctx context.Context, key string, body []byte) error {
		var order domain.OrderEvent
		if err := json.Unmarshal(body, &order); err != nil {
			// A malformed message is a data problem, not a service fault; log
			// and move on rather than stalling the partition.
			slog.Warn("skipping malformed order event",
				append(obs.LogTraceCtx(ctx), slog.Any("err", err))...)
			return nil
		}

		// Score the order over gRPC. This is the hop that carries the trace
		// context out of Kafka and down to risk-model.
		scoreCtx, cancel := grpcx.WithTimeout(ctx, d.Faults, d.Cfg.RequestTimeout)
		score, err := risk.Score(scoreCtx, &shopv1.ScoreRequest{
			OrderId:    order.OrderID,
			CustomerId: order.CustomerID,
			Amount:     order.Total.Proto(),
			Country:    order.Country,
		})
		cancel()
		if err != nil {
			return fmt.Errorf("score order %s: %w", order.OrderID, err)
		}

		template := "order_confirmation"
		if score.GetDecision() != "approve" {
			template = "order_under_review"
		}

		if err := producer.Publish(ctx, d.Cfg.TopicNotifications, order.OrderID, domain.NotificationEvent{
			OrderID:   order.OrderID,
			Email:     order.Email,
			Template:  template,
			Decision:  score.GetDecision(),
			RiskScore: score.GetScore(),
			Total:     order.Total,
			CreatedAt: time.Now().UTC(),
		}); err != nil {
			return fmt.Errorf("publish notification for %s: %w", order.OrderID, err)
		}

		slog.Debug("order scored",
			append(obs.LogTraceCtx(ctx),
				slog.String("order_id", order.OrderID),
				slog.String("decision", score.GetDecision()),
				slog.Float64("score", score.GetScore()))...)
		return nil
	}

	consumer, err := kafkax.NewConsumer(
		ctx, d.Cfg.KafkaBrokers, d.Cfg.GroupFraud, d.Cfg.TopicOrders, d.Faults, handler)
	if err != nil {
		return fmt.Errorf("orders consumer: %w", err)
	}
	defer func() { _ = consumer.Close() }()

	d.Admin.SetReady(true)
	return consumer.Run(ctx)
}
