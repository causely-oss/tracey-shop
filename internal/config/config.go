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

	// GenAI. The shopping assistant's provider configuration.
	//
	// GenAIAPI selects the wire protocol, not the vendor: "openai" is any
	// OpenAI-compatible /v1/chat/completions endpoint (OpenAI, Groq, Together,
	// OpenRouter, Fireworks, DeepSeek, xAI, Mistral, Ollama, vLLM, LiteLLM,
	// Azure OpenAI, Gemini's compat endpoint), and "anthropic" is the native
	// Messages API. The bundled model-gateway speaks the OpenAI format, so it is
	// not a third code path — it is "openai" pointed in-cluster with no key.
	GenAIEnabled     bool
	GenAIAPI         string
	GenAIBaseURL     string
	GenAIModel       string
	GenAISystem      string
	GenAIAPIKey      string
	GenAIMaxTokens   int
	GenAITemperature float64
	GenAITimeout     time.Duration

	// model-gateway shaping: the bundled provider's latency and how many output
	// tokens it claims to have produced.
	//
	// Named GATEWAY, never SIM. Env var names are part of the pod spec, which
	// Causely reads and quotes back in its remediation advice — a root cause
	// whose fix is "tune GENAI_SIM_LATENCY" tells the audience the incident was
	// staged, the same way a log line naming the injection would. Observed
	// verbatim in an AIModel Malfunction remediation before the rename.
	GenAIGatewayLatency      time.Duration
	GenAIGatewayOutputTokens int

	// Downstream genAI dependency, used by storefront-bff.
	AIAssistURL string

	// Load generator
	LoadTargetURL   string
	LoadRPS         float64
	LoadConcurrency int
	LoadMix         map[string]int
	// LoadAssistRPS paces genAI traffic independently of LoadRPS, in absolute
	// requests per second. Deliberately not a LoadMix weight: the mix is read
	// once at startup and would make genAI volume scale with total shop load,
	// so `scripts/load.sh 100` would silently multiply spend against a real
	// provider.
	//
	// The CHART is what turns this on — it ships 0.5, so the AI entities exist
	// in Causely from the first install. The 0 default here applies only to
	// running the binary standalone, where there is no model-gateway to answer.
	LoadAssistRPS float64
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

		GenAIEnabled:     envBool("GENAI_ENABLED", false),
		GenAIAPI:         env("GENAI_API", "openai"),
		GenAIBaseURL:     env("GENAI_BASE_URL", ""),
		GenAIModel:       env("GENAI_MODEL", ""),
		GenAISystem:      env("GENAI_SYSTEM", ""),
		GenAIAPIKey:      env("GENAI_API_KEY", ""),
		GenAIMaxTokens:   envInt("GENAI_MAX_TOKENS", 256),
		GenAITemperature: envFloat("GENAI_TEMPERATURE", 0.2),
		GenAITimeout:     envDuration("GENAI_TIMEOUT", 30*time.Second),

		// A real provider is never instant, and a flat floor gives Causely's
		// InferenceDuration tdigest a realistic shape rather than a spike at zero.
		GenAIGatewayLatency:      envDuration("GENAI_GATEWAY_LATENCY", 400*time.Millisecond),
		GenAIGatewayOutputTokens: envInt("GENAI_GATEWAY_OUTPUT_TOKENS", 48),

		AIAssistURL: env("AI_ASSIST_URL", "http://ai-assistant:8088"),

		LoadTargetURL:   env("LOAD_TARGET_URL", "http://storefront-bff:8080"),
		LoadRPS:         envFloat("LOAD_RPS", 20),
		LoadConcurrency: envInt("LOAD_CONCURRENCY", 16),
		LoadAssistRPS:   envFloat("LOAD_ASSIST_RPS", 0),
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
	c.normalizeGenAI()
	c.LoadMix = map[string]int{
		"browse":      envInt("LOAD_MIX_BROWSE", 50),
		"search":      envInt("LOAD_MIX_SEARCH", 20),
		"viewProduct": envInt("LOAD_MIX_VIEW_PRODUCT", 15),
		"addToCart":   envInt("LOAD_MIX_ADD_TO_CART", 10),
		"checkout":    envInt("LOAD_MIX_CHECKOUT", 5),
	}
	return c, nil
}

// normalizeGenAI fills in the provider-appropriate defaults.
//
// GENAI_SYSTEM matters more than it looks: its presence is the single trigger
// Causely uses to classify a span as genAI at all (it checks
// gen_ai.provider.name / gen_ai.system / llm.provider, before HTTP, RPC and DB).
// The *value* is never validated or even stored — but an empty one would leave
// the span looking like a plain HTTP call, so it must never end up blank.
func (c *Config) normalizeGenAI() {
	c.GenAIAPI = strings.ToLower(strings.TrimSpace(c.GenAIAPI))
	if c.GenAIAPI == "" {
		c.GenAIAPI = APIOpenAI
	}

	// Whether we are talking to the bundled model-gateway rather than a real
	// provider, which decides the default model name.
	//
	// It cannot be inferred from GENAI_BASE_URL alone: the chart ALWAYS sets
	// that, to the gateway's in-cluster URL when genai.external is empty. So the
	// chart states it explicitly, and the empty-URL check below is the fallback
	// for running the binary standalone. Getting this wrong is not cosmetic —
	// the model name becomes half the AIModel entity name in Causely, so the
	// demo would claim to be calling gpt-4o-mini while actually answering from
	// the in-cluster mock.
	bundled := c.GenAIBaseURL == ""
	if v, err := strconv.ParseBool(env("GENAI_BUNDLED", "")); err == nil {
		bundled = v
	}
	if c.GenAIBaseURL == "" {
		c.GenAIBaseURL = "http://model-gateway:8089"
	}
	c.GenAIBaseURL = strings.TrimSuffix(c.GenAIBaseURL, "/")

	if c.GenAIModel == "" {
		switch {
		case bundled:
			c.GenAIModel = "mock-small-1"
		case c.GenAIAPI == APIAnthropic:
			c.GenAIModel = "claude-haiku-4-5"
		default:
			c.GenAIModel = "gpt-4o-mini"
		}
	}

	if c.GenAISystem == "" {
		// The semconv well-known values for these two wire formats.
		switch c.GenAIAPI {
		case APIAnthropic:
			c.GenAISystem = "anthropic"
		default:
			c.GenAISystem = "openai"
		}
	}
}

// Wire protocols understood by internal/genai.
const (
	APIOpenAI    = "openai"
	APIAnthropic = "anthropic"
)

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
