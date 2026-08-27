// Package config loads all runtime configuration from the environment.
//
// Every service in the demo runs from the same binary, so this struct is the
// union of what any role needs. The Helm chart injects only the keys a given
// role actually uses; everything else falls back to a sensible in-cluster
// default so the binary is also runnable standalone for local debugging.
package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// Config is the fully-resolved runtime configuration for one pod.
type Config struct {
	// Identity
	Role        string // selects which service this process runs
	ServiceName string // OTel service.name; defaults to Role
	Namespace   string
	Version     string
	Env         string
	LogLevel    string

	// Listeners. Business ports are traced; the admin port never is.
	HTTPAddr  string
	GRPCAddr  string
	AdminAddr string

	// Telemetry
	OTLPEndpoint string
	SampleRatio  float64

	// Backends
	PostgresDSN  string
	RedisAddr    string
	RedisDB      int
	KafkaBrokers []string

	// Kafka topics and consumer groups
	TopicOrders        string
	TopicLedgerEvents  string
	TopicNotifications string
	GroupFraud         string
	GroupNotifications string

	// Downstream dependencies. gRPC targets are host:port, HTTP are full URLs.
	CatalogAddr   string
	CheckoutAddr  string
	PricingAddr   string
	InventoryAddr string
	PaymentAddr   string
	LedgerAddr    string
	RiskAddr      string

	CartURL     string
	ShippingURL string
	StripeURL   string
	CarrierURL  string
	EmailURL    string

	// Client behaviour
	RequestTimeout time.Duration

	// Order-intake backpressure on checkout-api. Ordinary application
	// behaviour, not a fault: while fraud review is far behind, intake sheds and
	// slows down. It is what gives a stalled consumer real service impact, and
	// therefore what lets Causely classify the Slow Consumer root cause as
	// Urgent instead of leaving it at its static weight. At a clean baseline the
	// backlog is near zero, so none of it engages.
	BackpressureEnabled      bool
	BackpressureLagThreshold int
	BackpressureRejectRate   float64
	BackpressureLatency      time.Duration
	BackpressurePollInterval time.Duration

	// partner-sim shaping: which third party this instance impersonates.
	PartnerName    string
	PartnerLatency time.Duration

	// Browser storefront, served by storefront-bff. The service named
	// "web-client" is a headless Go HTTP client, not a browser, so this is the
	// only real user-facing surface the demo has — and the only place a fault
	// shows up as something a person would actually see.
	WebUIEnabled bool

	// Load generator
	LoadTargetURL   string
	LoadRPS         float64
	LoadConcurrency int
	LoadMix         map[string]int
}

// Load reads the environment and applies defaults.
func Load() (*Config, error) {
	c := &Config{
		Role:        env("ROLE", ""),
		ServiceName: env("SERVICE_NAME", ""),
		Namespace:   env("POD_NAMESPACE", "tracey-shop"),
		Version:     env("SERVICE_VERSION", "0.1.0"),
		Env:         env("DEPLOY_ENV", "demo"),
		LogLevel:    env("LOG_LEVEL", "info"),

		HTTPAddr:  env("HTTP_ADDR", ""),
		GRPCAddr:  env("GRPC_ADDR", ""),
		AdminAddr: env("ADMIN_ADDR", ":8090"),

		OTLPEndpoint: env("OTEL_EXPORTER_OTLP_ENDPOINT", ""),
		SampleRatio:  envFloat("OTEL_TRACES_SAMPLER_RATIO", 1.0),

		PostgresDSN: env("POSTGRES_DSN", ""),
		RedisAddr:   env("REDIS_ADDR", ""),
		RedisDB:     envInt("REDIS_DB", 0),

		TopicOrders:        env("TOPIC_ORDERS", "orders"),
		TopicLedgerEvents:  env("TOPIC_LEDGER_EVENTS", "ledger.events"),
		TopicNotifications: env("TOPIC_NOTIFICATIONS", "notifications"),
		GroupFraud:         env("GROUP_FRAUD", "fraud-workers"),
		GroupNotifications: env("GROUP_NOTIFICATIONS", "notification-workers"),

		CatalogAddr:   env("CATALOG_ADDR", "catalog-api:9001"),
		CheckoutAddr:  env("CHECKOUT_ADDR", "checkout-api:9002"),
		PricingAddr:   env("PRICING_ADDR", "pricing-engine:9004"),
		InventoryAddr: env("INVENTORY_ADDR", "inventory-svc:9003"),
		PaymentAddr:   env("PAYMENT_ADDR", "payment-gw:9005"),
		LedgerAddr:    env("LEDGER_ADDR", "ledger-svc:9006"),
		RiskAddr:      env("RISK_ADDR", "risk-model:9007"),

		CartURL:     env("CART_URL", "http://cart-service:8081"),
		ShippingURL: env("SHIPPING_URL", "http://shipping-quote:8082"),
		StripeURL:   env("STRIPE_URL", "http://stripe-sim:8086"),
		CarrierURL:  env("CARRIER_URL", "http://carrier-sim:8085"),
		EmailURL:    env("EMAIL_URL", "http://email-sim:8087"),

		RequestTimeout: envDuration("REQUEST_TIMEOUT", 5*time.Second),

		BackpressureEnabled: envBool("BACKPRESSURE_ENABLED", true),
		// Set well above any transient backlog — a fraud-detector restart leaves
		// only tens of messages behind — so ordinary operation never sheds.
		BackpressureLagThreshold: envInt("BACKPRESSURE_LAG_THRESHOLD", 500),
		BackpressureRejectRate:   envFloat("BACKPRESSURE_REJECT_RATE", 0.25),
		BackpressureLatency:      envDuration("BACKPRESSURE_LATENCY", 300*time.Millisecond),
		BackpressurePollInterval: envDuration("BACKPRESSURE_POLL_INTERVAL", 15*time.Second),

		PartnerName:    env("PARTNER_NAME", ""),
		PartnerLatency: envDuration("PARTNER_LATENCY", 25*time.Millisecond),

		WebUIEnabled: envBool("WEB_UI_ENABLED", true),

		LoadTargetURL:   env("LOAD_TARGET_URL", "http://storefront-bff:8080"),
		LoadRPS:         envFloat("LOAD_RPS", 20),
		LoadConcurrency: envInt("LOAD_CONCURRENCY", 16),
	}

	if c.Role == "" {
		return nil, fmt.Errorf("ROLE is required")
	}
	if c.ServiceName == "" {
		c.ServiceName = c.Role
	}
	if brokers := env("KAFKA_BROKERS", "kafka:9092"); brokers != "" {
		c.KafkaBrokers = splitAndTrim(brokers)
	}
	c.LoadMix = map[string]int{
		"browse":      envInt("LOAD_MIX_BROWSE", 50),
		"search":      envInt("LOAD_MIX_SEARCH", 20),
		"viewProduct": envInt("LOAD_MIX_VIEW_PRODUCT", 15),
		"addToCart":   envInt("LOAD_MIX_ADD_TO_CART", 10),
		"checkout":    envInt("LOAD_MIX_CHECKOUT", 5),
	}
	return c, nil
}

func env(key, def string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return def
}

func envInt(key string, def int) int {
	if v, err := strconv.Atoi(env(key, "")); err == nil {
		return v
	}
	return def
}

func envBool(key string, def bool) bool {
	if v, err := strconv.ParseBool(env(key, "")); err == nil {
		return v
	}
	return def
}

func envFloat(key string, def float64) float64 {
	if v, err := strconv.ParseFloat(env(key, ""), 64); err == nil {
		return v
	}
	return def
}

func envDuration(key string, def time.Duration) time.Duration {
	if v, err := time.ParseDuration(env(key, "")); err == nil {
		return v
	}
	return def
}

func splitAndTrim(s string) []string {
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}
