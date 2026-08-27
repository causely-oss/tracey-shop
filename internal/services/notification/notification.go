// Package notification implements notification-worker: a Kafka consumer that
// hands each notification to the email provider over HTTP.
//
// It closes the asynchronous branch — Kafka in, HTTP out — so the demo has a
// consumer whose downstream is a synchronous third party.
package notification

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"

	"github.com/causely-oss/tracey-shop/internal/app"
	"github.com/causely-oss/tracey-shop/internal/domain"
	"github.com/causely-oss/tracey-shop/internal/obs"
	"github.com/causely-oss/tracey-shop/internal/transport/kafkax"
)

// Run starts the notification-worker consumer loop.
func Run(ctx context.Context, d *app.Deps) error {
	email := d.HTTPClient(d.Cfg.EmailURL)

	handler := func(ctx context.Context, key string, body []byte) error {
		var event domain.NotificationEvent
		if err := json.Unmarshal(body, &event); err != nil {
			slog.Warn("skipping malformed notification",
				append(obs.LogTraceCtx(ctx), slog.Any("err", err))...)
			return nil
		}

		recipient := event.Email
		if recipient == "" {
			recipient = "customer@example.com"
		}

		var sent domain.PartnerResponse
		if err := email.PostJSON(ctx, "/messages", domain.PartnerRequest{
			Reference: event.OrderID,
			To:        recipient,
			Metadata: map[string]string{
				"template": event.Template,
				"decision": event.Decision,
			},
		}, &sent); err != nil {
			return fmt.Errorf("send notification for %s: %w", event.OrderID, err)
		}

		slog.Debug("notification sent",
			append(obs.LogTraceCtx(ctx),
				slog.String("order_id", event.OrderID),
				slog.String("template", event.Template),
				slog.String("message_id", sent.Code))...)
		return nil
	}

	consumer, err := kafkax.NewConsumer(
		ctx, d.Cfg.KafkaBrokers, d.Cfg.GroupNotifications, d.Cfg.TopicNotifications, d.Faults, handler)
	if err != nil {
		return fmt.Errorf("notifications consumer: %w", err)
	}
	defer func() { _ = consumer.Close() }()

	d.Admin.SetReady(true)
	return consumer.Run(ctx)
}
