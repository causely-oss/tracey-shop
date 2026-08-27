// Package kafkax provides Kafka producer and consumer helpers.
//
// Spans here are created by hand rather than via an instrumentation library,
// because Causely's messaging analysis depends on a precise attribute set and
// library semconv vintages drift. A PRODUCER span becomes a "Produces" edge to
// the topic and a CONSUMER span becomes a "Consumes" edge, but only when the
// span carries messaging.destination.name plus a resolvable broker address.
// messaging.consumer.group.name is what lets Causely attribute consumer lag to
// the right group.
//
// Trace context travels in Kafka headers via the W3C propagator, so the async
// leg of the demo (checkout -> orders -> fraud -> notifications -> worker) shows
// up as one connected trace.
package kafkax

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"strconv"
	"time"

	"github.com/IBM/sarama"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	semconv "go.opentelemetry.io/otel/semconv/v1.34.0"
	"go.opentelemetry.io/otel/trace"

	"github.com/causely-oss/tracey-shop/internal/faults"
	"github.com/causely-oss/tracey-shop/internal/obs"
)

// ---------------------------------------------------------------------------
// Producer
// ---------------------------------------------------------------------------

// Producer publishes JSON messages with trace context in the headers.
type Producer struct {
	sp         sarama.SyncProducer
	brokerHost string
	brokerPort int
	tracer     trace.Tracer
}

// waitForBroker retries connect until Kafka accepts, mirroring what pgxx and
// redisx do for Postgres and Valkey.
//
// Without this, a cold `helm install` reliably restarts every Kafka-dependent
// pod once: sarama's own metadata retries cover about ten seconds, Kafka's KRaft
// broker takes longer than that to come up, and the constructor's error is fatal
// to the process. The pods recover on their own, but they leave RESTARTS: 1
// behind — which is indistinguishable from a real symptom in Causely and breaks
// the clean baseline the demo depends on.
func waitForBroker(ctx context.Context, brokers []string, cfg *sarama.Config, what string) error {
	deadline := time.Now().Add(5 * time.Minute)
	var lastErr error
	for time.Now().Before(deadline) {
		client, err := sarama.NewClient(brokers, cfg)
		if err == nil {
			_ = client.Close()
			return nil
		}
		lastErr = err
		slog.Info("waiting for kafka",
			slog.String("for", what),
			slog.Any("brokers", brokers),
			slog.Any("err", err))
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(3 * time.Second):
		}
	}
	return fmt.Errorf("kafka not ready for %s: %w", what, lastErr)
}

func baseConfig() *sarama.Config {
	cfg := sarama.NewConfig()
	cfg.Version = sarama.V3_6_0_0
	cfg.Metadata.Retry.Max = 10
	cfg.Metadata.Retry.Backoff = 1 * time.Second
	return cfg
}

// NewProducer connects a synchronous producer to the given brokers, waiting for
// the broker to become available first.
func NewProducer(ctx context.Context, brokers []string) (*Producer, error) {
	cfg := baseConfig()
	cfg.Producer.RequiredAcks = sarama.WaitForLocal
	cfg.Producer.Return.Successes = true
	cfg.Producer.Retry.Max = 3
	cfg.Producer.Retry.Backoff = 200 * time.Millisecond

	if err := waitForBroker(ctx, brokers, cfg, "producer"); err != nil {
		return nil, err
	}

	sp, err := sarama.NewSyncProducer(brokers, cfg)
	if err != nil {
		return nil, fmt.Errorf("new sync producer: %w", err)
	}

	host, port := splitBroker(brokers[0])
	slog.Info("kafka producer ready", slog.String("broker", brokers[0]))
	return &Producer{
		sp:         sp,
		brokerHost: host,
		brokerPort: port,
		tracer:     obs.Tracer("kafkax"),
	}, nil
}

// Publish marshals payload to JSON and sends it to topic under a PRODUCER span.
func (p *Producer) Publish(ctx context.Context, topic, key string, payload any) error {
	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal %s message: %w", topic, err)
	}

	ctx, span := p.tracer.Start(ctx,
		"publish "+topic,
		trace.WithSpanKind(trace.SpanKindProducer),
		trace.WithAttributes(
			semconv.MessagingSystemKafka,
			semconv.MessagingDestinationName(topic),
			semconv.MessagingOperationName("publish"),
			semconv.MessagingOperationTypePublish,
			semconv.ServerAddress(p.brokerHost),
			semconv.ServerPort(p.brokerPort),
			semconv.MessagingMessageBodySize(len(body)),
		),
	)
	defer span.End()

	msg := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.ByteEncoder(body),
	}
	if key != "" {
		msg.Key = sarama.StringEncoder(key)
		span.SetAttributes(semconv.MessagingKafkaMessageKey(key))
	}

	// Inject W3C trace context so the consumer's span is a child of this one.
	carrier := headerCarrier{msg: msg}
	otel.GetTextMapPropagator().Inject(ctx, carrier)

	partition, offset, err := p.sp.SendMessage(msg)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return fmt.Errorf("send to %s: %w", topic, err)
	}

	span.SetAttributes(
		semconv.MessagingDestinationPartitionID(strconv.Itoa(int(partition))),
		semconv.MessagingKafkaOffset(int(offset)),
	)
	return nil
}

// Close shuts the producer down.
func (p *Producer) Close() error { return p.sp.Close() }

// ---------------------------------------------------------------------------
// Consumer
// ---------------------------------------------------------------------------

// HandlerFunc processes one message body. Returning an error marks the CONSUMER
// span as failed but still commits, keeping the demo from wedging on a single
// bad message.
type HandlerFunc func(ctx context.Context, key string, body []byte) error

// Consumer is a consumer-group member for one topic.
type Consumer struct {
	group      sarama.ConsumerGroup
	groupID    string
	topic      string
	brokerHost string
	brokerPort int
	handler    HandlerFunc
	faults     *faults.Store
	tracer     trace.Tracer
}

// NewConsumer joins groupID and consumes topic, waiting for the broker first.
func NewConsumer(ctx context.Context, brokers []string, groupID, topic string, store *faults.Store, h HandlerFunc) (*Consumer, error) {
	cfg := baseConfig()
	cfg.Consumer.Offsets.Initial = sarama.OffsetOldest
	cfg.Consumer.Return.Errors = true
	cfg.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{sarama.NewBalanceStrategyRoundRobin()}

	if err := waitForBroker(ctx, brokers, cfg, "consumer group "+groupID); err != nil {
		return nil, err
	}

	group, err := sarama.NewConsumerGroup(brokers, groupID, cfg)
	if err != nil {
		return nil, fmt.Errorf("new consumer group %s: %w", groupID, err)
	}

	host, port := splitBroker(brokers[0])
	return &Consumer{
		group:      group,
		groupID:    groupID,
		topic:      topic,
		brokerHost: host,
		brokerPort: port,
		handler:    h,
		faults:     store,
		tracer:     obs.Tracer("kafkax"),
	}, nil
}

// Run consumes until the context is cancelled.
func (c *Consumer) Run(ctx context.Context) error {
	go func() {
		for err := range c.group.Errors() {
			slog.Error("consumer group error", slog.String("group", c.groupID), slog.Any("err", err))
		}
	}()

	slog.Info("kafka consumer started",
		slog.String("group", c.groupID),
		slog.String("topic", c.topic))

	for {
		if err := c.group.Consume(ctx, []string{c.topic}, c); err != nil {
			if ctx.Err() != nil {
				return nil
			}
			slog.Error("consume returned", slog.Any("err", err))
			select {
			case <-ctx.Done():
				return nil
			case <-time.After(2 * time.Second):
			}
			continue
		}
		if ctx.Err() != nil {
			return nil
		}
	}
}

// Close shuts the consumer group down.
func (c *Consumer) Close() error { return c.group.Close() }

// Setup implements sarama.ConsumerGroupHandler.
func (c *Consumer) Setup(sarama.ConsumerGroupSession) error { return nil }

// Cleanup implements sarama.ConsumerGroupHandler.
func (c *Consumer) Cleanup(sarama.ConsumerGroupSession) error { return nil }

// ConsumeClaim implements sarama.ConsumerGroupHandler.
func (c *Consumer) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for {
		select {
		case <-sess.Context().Done():
			return nil
		case msg, ok := <-claim.Messages():
			if !ok {
				return nil
			}

			// The consumerStall fault stops progress without crashing, so the
			// observable symptom is consumer lag on the topic rather than an
			// error rate — which is what makes it a distinct demo scenario.
			//
			// It is also the only scenario with no synchronous caller to report
			// an error, so this log is the sole textual evidence of why lag is
			// growing. It is rate-limited, so the loop can log every iteration.
			if c.faults != nil && c.faults.ConsumerStalled() {
				// Emit the CONSUMER span but neither process the message nor
				// commit its offset.
				//
				// The span is not optional. Causely attaches consumer lag to the
				// ConsumerTopicAccess entity, and that entity exists *only*
				// because of these spans — the lag metric can never create it.
				// An earlier version blocked here before creating any span, so
				// while stalled the entity stopped being refreshed; after a
				// mediator restart it vanished entirely and the scenario went
				// permanently silent even though lag kept climbing.
				c.observeStalled(sess.Context(), msg)

				// Not calling sess.MarkMessage is what actually produces the
				// lag: the committed offset stays put while the log end advances.
				select {
				case <-sess.Context().Done():
					return nil
				case <-time.After(stallPace):
				}
				continue
			}

			c.handleMessage(sess, msg)
		}
	}
}

// stallPace throttles the stalled loop so a large backlog does not spin.
const stallPace = 500 * time.Millisecond

// startConsumerSpan creates the CONSUMER span for a message, linked to the
// producer via the W3C headers it injected.
//
// Shared by the healthy and stalled paths deliberately: the attributes here are
// what create the Topic and ConsumerTopicAccess entities in Causely, and
// messaging.consumer.group.name is what becomes the ConsumerGroup label that
// consumer-lag metrics are matched against. Both paths must emit an identical
// span or the entity changes shape when a fault is active.
func (c *Consumer) startConsumerSpan(parent context.Context, msg *sarama.ConsumerMessage) (context.Context, trace.Span) {
	ctx := otel.GetTextMapPropagator().Extract(parent, consumerHeaderCarrier{msg: msg})

	return c.tracer.Start(ctx,
		"process "+msg.Topic,
		trace.WithSpanKind(trace.SpanKindConsumer),
		trace.WithAttributes(
			semconv.MessagingSystemKafka,
			semconv.MessagingDestinationName(msg.Topic),
			semconv.MessagingOperationName("process"),
			semconv.MessagingOperationTypeProcess,
			semconv.MessagingConsumerGroupName(c.groupID),
			semconv.MessagingDestinationPartitionID(strconv.Itoa(int(msg.Partition))),
			semconv.MessagingKafkaOffset(int(msg.Offset)),
			semconv.MessagingMessageBodySize(len(msg.Value)),
			semconv.ServerAddress(c.brokerHost),
			semconv.ServerPort(c.brokerPort),
		),
	)
}

// observeStalled records a message that was received but deliberately neither
// processed nor committed, keeping the ConsumerTopicAccess entity alive in
// Causely so the lag metric has something to attach to.
func (c *Consumer) observeStalled(parent context.Context, msg *sarama.ConsumerMessage) {
	_, span := c.startConsumerSpan(parent, msg)
	defer span.End()

	c.faults.LogConsumerStall(msg.Topic, c.groupID, msg.Partition, msg.Offset)

	// Marking the span failed is what gives the stall a request-side signal in
	// addition to the lag metric.
	err := fmt.Errorf("message not processed: consumer is not making progress on %s", msg.Topic)
	span.RecordError(err)
	span.SetStatus(codes.Error, err.Error())
}

func (c *Consumer) handleMessage(sess sarama.ConsumerGroupSession, msg *sarama.ConsumerMessage) {
	ctx, span := c.startConsumerSpan(sess.Context(), msg)
	defer span.End()

	if err := c.handler(ctx, string(msg.Key), msg.Value); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		slog.Error("message handler failed",
			append(obs.LogTraceCtx(ctx),
				slog.String("topic", msg.Topic),
				slog.Any("err", err))...)
	}

	sess.MarkMessage(msg, "")
}

// ---------------------------------------------------------------------------
// Propagation carriers
// ---------------------------------------------------------------------------

type headerCarrier struct{ msg *sarama.ProducerMessage }

func (c headerCarrier) Get(key string) string {
	for _, h := range c.msg.Headers {
		if string(h.Key) == key {
			return string(h.Value)
		}
	}
	return ""
}

func (c headerCarrier) Set(key, value string) {
	for i, h := range c.msg.Headers {
		if string(h.Key) == key {
			c.msg.Headers[i].Value = []byte(value)
			return
		}
	}
	c.msg.Headers = append(c.msg.Headers, sarama.RecordHeader{
		Key:   []byte(key),
		Value: []byte(value),
	})
}

func (c headerCarrier) Keys() []string {
	keys := make([]string, 0, len(c.msg.Headers))
	for _, h := range c.msg.Headers {
		keys = append(keys, string(h.Key))
	}
	return keys
}

type consumerHeaderCarrier struct{ msg *sarama.ConsumerMessage }

func (c consumerHeaderCarrier) Get(key string) string {
	for _, h := range c.msg.Headers {
		if h != nil && string(h.Key) == key {
			return string(h.Value)
		}
	}
	return ""
}

func (c consumerHeaderCarrier) Set(string, string) {}

func (c consumerHeaderCarrier) Keys() []string {
	keys := make([]string, 0, len(c.msg.Headers))
	for _, h := range c.msg.Headers {
		if h != nil {
			keys = append(keys, string(h.Key))
		}
	}
	return keys
}

var (
	_ propagation.TextMapCarrier = headerCarrier{}
	_ propagation.TextMapCarrier = consumerHeaderCarrier{}
	_ attribute.KeyValue         = semconv.MessagingSystemKafka
)

func splitBroker(broker string) (string, int) {
	host, portStr, err := net.SplitHostPort(broker)
	if err != nil {
		return broker, 9092
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		return host, 9092
	}
	return host, port
}
