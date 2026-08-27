package faults

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"
)

// Narrative is the set of log messages a service emits while a fault is active.
//
// Causely ingests container logs and uses WARN/ERROR lines as the evidence it
// synthesises a root-cause description from. Without these, a scenario produces
// only metric symptoms and the resulting description is generic ("inspect the
// application logs..."). With them, Causely can name the actual failure mode.
//
// Two rules for every message here:
//
//  1. It must read like a real application error. Nothing may mention faults,
//     injection or scenarios — a demo where the root cause is "fault spec
//     updated" tells the audience the incident was staged. This is not
//     hypothetical: an earlier version logged "fault spec updated" at WARN and
//     Causely dutifully reported the root cause as "Fault spec updated causing
//     payment authorization malfunction", with remediation "revert the fault
//     specification update".
//  2. It must be specific enough to be useful — the failing operation, the
//     dependency, and the numbers a real engineer would want.
type Narrative struct {
	// Error is logged at ERROR when the errorRate fault fails a request.
	Error string
	// Latency is logged at WARN when the latency fault delays a request.
	Latency string
	// CPU is logged at WARN when the cpuBurn fault consumes its budget.
	CPU string
	// Memory is logged at WARN as the memory leak grows.
	Memory string
	// Panic is the panic message itself, so a crash's stack trace reads
	// plausibly rather than announcing the injection.
	Panic string
	// SlowQuery is logged at WARN when a database call is slowed.
	SlowQuery string
	// PoolExhausted is logged at ERROR when leaked connections starve the pool.
	PoolExhausted string
	// ConsumerStall is logged at ERROR when a Kafka consumer stops progressing.
	ConsumerStall string
	// CacheBypass is logged at WARN when cache reads are skipped.
	CacheBypass string
	// DependencyTimeout is logged at ERROR when an outbound call is cut short.
	DependencyTimeout string
}

// defaultNarrative is used for services that no scenario targets directly.
var defaultNarrative = Narrative{
	Error:             "request handling failed",
	Latency:           "request latency degraded",
	CPU:               "request exceeded its CPU budget",
	Memory:            "heap growth detected in request path",
	Panic:             "unrecoverable error in request handler",
	SlowQuery:         "database query exceeded its budget",
	PoolExhausted:     "database connection pool exhausted",
	ConsumerStall:     "message processing halted",
	CacheBypass:       "cache unavailable, falling through to origin",
	DependencyTimeout: "downstream call exceeded its deadline",
}

// narratives is keyed by service name (SERVICE_NAME, which is what
// scripts/scenario.sh targets), not by role — so the three partner simulators
// can differ from one another despite sharing an implementation.
var narratives = map[string]Narrative{
	// --- payment-errors ----------------------------------------------------
	"payment-gw": {
		Error: "payment authorization rejected by acquirer: no auth code returned",
		// cart-timeouts style cascade, if ever pointed here.
		DependencyTimeout: "ledger settlement call exceeded deadline, authorization abandoned",
		Latency:           "payment authorization latency degraded",
		Panic:             "unrecoverable error settling authorization",
	},

	// --- ledger-slow-queries, ledger-pool-exhaustion -----------------------
	"ledger-svc": {
		SlowQuery:     "double-entry journal write exceeded its query budget",
		PoolExhausted: "ledger connection pool exhausted, journal writes are queueing",
		Error:         "journal write failed, transaction rolled back",
		Latency:       "journal write latency degraded",
	},

	// --- inventory-oom -----------------------------------------------------
	"inventory-svc": {
		Memory:    "stock projection cache is growing without bound",
		SlowQuery: "stock availability query exceeded its budget",
		Error:     "stock reservation failed, could not acquire row lock",
	},

	// --- fraud-lag ---------------------------------------------------------
	"fraud-detector": {
		// The only signal for this scenario: there is no synchronous caller to
		// report an error, so the log is what explains the growing lag.
		ConsumerStall: "order event processing halted, offsets are no longer committing",
		Error:         "fraud scoring failed for order event",
	},

	// --- pricing-cpu -------------------------------------------------------
	"pricing-engine": {
		CPU:       "price rule evaluation exceeded its CPU budget",
		Error:     "price quote failed during rule evaluation",
		SlowQuery: "price rule lookup exceeded its query budget",
	},

	// --- cart-timeouts (the slow side) -------------------------------------
	"cart-service": {
		Latency: "cart read latency degraded against the session store",
		Error:   "cart read failed against the session store",
	},

	// --- cart-timeouts (the erroring side), checkout-latency ---------------
	"checkout-api": {
		// This is the interesting one: checkout is the victim, and this log is
		// what should stop Causely blaming it for cart-service's latency.
		DependencyTimeout: "downstream call exceeded deadline, abandoning order placement",
		Error:             "order placement failed",
		Latency:           "checkout orchestration latency degraded",
	},

	// --- catalog-cache-miss ------------------------------------------------
	"catalog-api": {
		CacheBypass: "product cache unavailable, serving reads from inventory-svc",
		Error:       "catalogue read failed",
	},

	// --- risk-crash --------------------------------------------------------
	"risk-model": {
		Panic: "unrecoverable error scoring order: feature vector dimension mismatch",
		Error: "risk scoring failed",
	},

	// --- not directly targeted, but plausible if used ----------------------
	"storefront-bff": {
		Error:             "request failed at the storefront edge",
		DependencyTimeout: "downstream call exceeded deadline, returning error to client",
	},
	"shipping-quote": {
		Error:             "carrier rate lookup failed",
		DependencyTimeout: "carrier API call exceeded deadline",
	},
	"notification-worker": {
		Error:         "notification delivery failed",
		ConsumerStall: "notification processing halted, offsets are no longer committing",
	},
	"stripe-sim": {
		Error: "charge request rejected",
	},
	"carrier-sim": {
		Error: "shipment booking rejected",
	},
	"email-sim": {
		Error: "message delivery rejected",
	},
}

// narrativeFor returns the service's messages, filling any blank field from the
// default set so no code path logs an empty message.
func narrativeFor(service string) Narrative {
	n, ok := narratives[service]
	if !ok {
		return defaultNarrative
	}
	d := defaultNarrative
	if n.Error == "" {
		n.Error = d.Error
	}
	if n.Latency == "" {
		n.Latency = d.Latency
	}
	if n.CPU == "" {
		n.CPU = d.CPU
	}
	if n.Memory == "" {
		n.Memory = d.Memory
	}
	if n.Panic == "" {
		n.Panic = d.Panic
	}
	if n.SlowQuery == "" {
		n.SlowQuery = d.SlowQuery
	}
	if n.PoolExhausted == "" {
		n.PoolExhausted = d.PoolExhausted
	}
	if n.ConsumerStall == "" {
		n.ConsumerStall = d.ConsumerStall
	}
	if n.CacheBypass == "" {
		n.CacheBypass = d.CacheBypass
	}
	if n.DependencyTimeout == "" {
		n.DependencyTimeout = d.DependencyTimeout
	}
	return n
}

// logInterval is the minimum gap between two emissions of the same message.
//
// Rate limiting is essential here: pricing-cpu fires on the browse path, which
// is ~85% of traffic, so logging every occurrence at 40 rps would emit tens of
// lines per second per pod. Causely needs a representative sample, not the
// firehose — and each emission carries a suppressed count so the true rate is
// still visible.
const logInterval = 5 * time.Second

// logLimiter throttles repeated messages and counts what it dropped.
type logLimiter struct {
	mu         sync.Mutex
	last       map[string]time.Time
	suppressed map[string]int
}

func newLogLimiter() *logLimiter {
	return &logLimiter{
		last:       make(map[string]time.Time),
		suppressed: make(map[string]int),
	}
}

// allow reports whether the message should be emitted now, and how many
// occurrences were suppressed since the previous emission.
func (l *logLimiter) allow(key string) (bool, int) {
	l.mu.Lock()
	defer l.mu.Unlock()

	now := time.Now()
	last, seen := l.last[key]
	if seen && now.Sub(last) < logInterval {
		l.suppressed[key]++
		return false, 0
	}
	dropped := l.suppressed[key]
	l.suppressed[key] = 0
	l.last[key] = now
	return true, dropped
}

// emit logs msg at the given level, rate-limited per message, with the standard
// suppression counter appended.
func (s *Store) emit(level slog.Level, msg string, attrs ...any) {
	allowed, dropped := s.limiter.allow(msg)
	if !allowed {
		return
	}
	if dropped > 0 {
		attrs = append(attrs,
			slog.Int("suppressed_similar", dropped),
			slog.String("suppression_window", logInterval.String()))
	}
	// No "service" attribute here: obs.SetupLogging already binds it to the
	// default logger, and adding it again emits the key twice per line.
	slog.Log(context.Background(), level, msg, attrs...)
}

// durationAttr formats a duration as integer milliseconds.
func durationAttr(key string, d time.Duration) slog.Attr {
	return slog.Int64(key, d.Milliseconds())
}

// pct renders a ratio as a percentage string, for log readability.
func pct(f float64) string {
	return fmt.Sprintf("%.1f%%", f*100)
}
