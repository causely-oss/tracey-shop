// Package faults provides runtime-toggleable failure injection.
//
// Every pod holds one Store. It starts empty, so a fresh `helm install` runs a
// completely clean demo. Faults are turned on by POSTing to the pod's admin
// endpoint (see scripts/scenario.sh) — no helm upgrade and no restart, so a
// live demo can move from healthy to broken and back in seconds.
package faults

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"math/rand"
	"sync"
	"time"
)

// ErrInjected identifies an error produced by the error-rate fault. Callers
// translate it into an HTTP 500 or a gRPC INTERNAL, and match it with
// errors.Is(err, ErrInjected).
//
// Its text is deliberately neutral and never reaches a caller — see
// injectedError. gRPC error strings propagate all the way up to the browser and
// into the JSON body storefront-bff returns, so a sentinel reading "injected
// fault" put the words on screen in front of the demo audience. Same class of
// leak as the "fault spec updated" log line.
var ErrInjected = errors.New("request rejected")

// injectedError carries only the service's domain-plausible narrative message
// while staying identifiable as an injected fault via errors.Is.
type injectedError struct{ msg string }

func (e *injectedError) Error() string { return e.msg }

// Is lets errors.Is(err, ErrInjected) succeed without ErrInjected's text ever
// being wrapped into the message.
func (e *injectedError) Is(target error) bool { return target == ErrInjected }

// Spec is the full set of faults a service can be asked to exhibit.
type Spec struct {
	// ErrorRate is the fraction of requests failed outright, 0..1.
	ErrorRate float64 `json:"errorRate"`
	// LatencyMs is added to every request, with up to LatencyJitterMs extra.
	LatencyMs       int `json:"latencyMs"`
	LatencyJitterMs int `json:"latencyJitterMs"`
	// SlowQueryMs makes each database call sleep server-side, so the delay
	// shows up in Postgres' own statistics and in Causely's slow-query view.
	SlowQueryMs int `json:"slowQueryMs"`
	// DBConnLeak leaks one pool connection per request until the pool starves.
	DBConnLeak bool `json:"dbConnLeak"`
	// MemLeakKBPerReq retains memory per request until the container is OOMKilled.
	MemLeakKBPerReq int `json:"memLeakKbPerReq"`
	// CPUBurnMs spins the CPU per request, provoking CFS throttling.
	CPUBurnMs int `json:"cpuBurnMs"`
	// ConsumerStall makes a Kafka consumer stop making progress, building lag.
	ConsumerStall bool `json:"consumerStall"`
	// DependencyTimeoutMs shortens outbound client timeouts, so a mildly slow
	// dependency turns into a hard failure that cascades.
	DependencyTimeoutMs int `json:"dependencyTimeoutMs"`
	// PanicRate crashes the process, producing CrashLoopBackOff and restarts.
	PanicRate float64 `json:"panicRate"`
	// DisableCache forces cache misses, shifting read load onto Postgres.
	DisableCache bool `json:"disableCache"`
}

// IsZero reports whether the spec asks for nothing at all.
func (s Spec) IsZero() bool {
	return s == Spec{}
}

// Patch is a partial update; nil fields are left untouched.
type Patch struct {
	ErrorRate           *float64 `json:"errorRate"`
	LatencyMs           *int     `json:"latencyMs"`
	LatencyJitterMs     *int     `json:"latencyJitterMs"`
	SlowQueryMs         *int     `json:"slowQueryMs"`
	DBConnLeak          *bool    `json:"dbConnLeak"`
	MemLeakKBPerReq     *int     `json:"memLeakKbPerReq"`
	CPUBurnMs           *int     `json:"cpuBurnMs"`
	ConsumerStall       *bool    `json:"consumerStall"`
	DependencyTimeoutMs *int     `json:"dependencyTimeoutMs"`
	PanicRate           *float64 `json:"panicRate"`
	DisableCache        *bool    `json:"disableCache"`
}

// Store holds the active spec plus the state that leak-style faults accumulate.
type Store struct {
	mu   sync.RWMutex
	spec Spec

	// ballast retains leaked memory for MemLeakKBPerReq.
	ballast [][]byte
	// leakedConns holds release funcs we deliberately never call.
	leakedConns []func()

	rng   *rand.Rand
	rngMu sync.Mutex

	// service is the SERVICE_NAME this store belongs to; it selects the
	// narrative used for the logs Causely reads as root-cause evidence.
	service   string
	narrative Narrative
	limiter   *logLimiter
}

// NewStore returns an empty store — no faults active. service should be the
// pod's SERVICE_NAME, which selects its log narrative.
func NewStore(service string) *Store {
	return &Store{
		rng:       rand.New(rand.NewSource(time.Now().UnixNano())),
		service:   service,
		narrative: narrativeFor(service),
		limiter:   newLogLimiter(),
	}
}

// Get returns the current spec.
func (s *Store) Get() Spec {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.spec
}

// Apply merges a patch and returns the resulting spec.
func (s *Store) Apply(p Patch) Spec {
	s.mu.Lock()
	defer s.mu.Unlock()
	setIf(&s.spec.ErrorRate, p.ErrorRate)
	setIf(&s.spec.LatencyMs, p.LatencyMs)
	setIf(&s.spec.LatencyJitterMs, p.LatencyJitterMs)
	setIf(&s.spec.SlowQueryMs, p.SlowQueryMs)
	setIf(&s.spec.DBConnLeak, p.DBConnLeak)
	setIf(&s.spec.MemLeakKBPerReq, p.MemLeakKBPerReq)
	setIf(&s.spec.CPUBurnMs, p.CPUBurnMs)
	setIf(&s.spec.ConsumerStall, p.ConsumerStall)
	setIf(&s.spec.DependencyTimeoutMs, p.DependencyTimeoutMs)
	setIf(&s.spec.PanicRate, p.PanicRate)
	setIf(&s.spec.DisableCache, p.DisableCache)
	// Deliberately Debug, not Warn.
	//
	// Causely ingests container logs and uses WARN/ERROR lines as evidence when
	// it synthesises a root cause. With this at Warn, a demo of the
	// payment-errors scenario produced the root cause "Fault spec updated
	// causing payment authorization malfunction", with remediation "revert the
	// fault specification update" — technically correct, but it exposes the
	// injection mechanism and reads as staged rather than as a real incident.
	// At Debug it stays out of the evidence at the default LOG_LEVEL=info, and
	// Causely diagnoses the service from its actual error-rate symptoms instead.
	slog.Debug("fault spec updated", slog.Any("spec", s.spec))
	return s.spec
}

// Clear resets every fault and releases the memory and connections that the
// leak faults accumulated, so a service recovers without a restart.
func (s *Store) Clear() Spec {
	s.mu.Lock()
	releases := s.leakedConns
	s.leakedConns = nil
	s.ballast = nil
	s.spec = Spec{}
	s.mu.Unlock()

	for _, release := range releases {
		release()
	}
	// Debug for the same reason as in Apply: keep the injection mechanism out of
	// Causely's log-derived evidence.
	slog.Debug("fault spec cleared")
	return Spec{}
}

// Gate runs the pre-handler faults: latency, CPU burn, memory leak, panic and
// the error-rate roll. It returns ErrInjected when the request should fail.
//
// Call this at the top of every business handler.
func (s *Store) Gate(ctx context.Context) error {
	spec := s.Get()
	if spec.IsZero() {
		return nil
	}

	if spec.PanicRate > 0 && s.roll() < spec.PanicRate {
		// Deliberate: surfaces as a pod restart / CrashLoopBackOff. The message
		// is domain-plausible so the stack trace reads like a real crash.
		slog.Error(s.narrative.Panic, slog.String("service", s.service))
		panic(s.narrative.Panic)
	}

	if spec.MemLeakKBPerReq > 0 {
		chunk := make([]byte, spec.MemLeakKBPerReq*1024)
		for i := range chunk {
			chunk[i] = byte(i) // touch pages so the RSS actually grows
		}
		s.mu.Lock()
		s.ballast = append(s.ballast, chunk)
		retained := len(s.ballast)
		s.mu.Unlock()

		s.emit(slog.LevelWarn, s.narrative.Memory,
			slog.Int("retained_entries", retained),
			slog.Int64("retained_mb", int64(retained)*int64(spec.MemLeakKBPerReq)/1024),
			slog.Int("bytes_per_entry", spec.MemLeakKBPerReq*1024))
	}

	if spec.CPUBurnMs > 0 {
		burnCPU(time.Duration(spec.CPUBurnMs) * time.Millisecond)
		s.emit(slog.LevelWarn, s.narrative.CPU,
			slog.Int("cpu_ms", spec.CPUBurnMs),
			slog.Int("budget_ms", 10))
	}

	if d := s.latency(spec); d > 0 {
		select {
		case <-time.After(d):
			s.emit(slog.LevelWarn, s.narrative.Latency,
				durationAttr("duration_ms", d),
				slog.Int("threshold_ms", 250))
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	if spec.ErrorRate > 0 && s.roll() < spec.ErrorRate {
		s.emit(slog.LevelError, s.narrative.Error,
			slog.String("observed_failure_rate", pct(spec.ErrorRate)),
			slog.Bool("retryable", true))
		// The narrative message only. Anything added here travels up the gRPC
		// chain into the JSON body the browser renders.
		return &injectedError{msg: s.narrative.Error}
	}
	return nil
}

// ---------------------------------------------------------------------------
// Logs emitted from the transport layers
//
// These are separate from Gate because the conditions they describe are
// detected where the work happens — in the database pool, the Kafka consume
// loop, the cache read and the HTTP client — not in the request gate.
// ---------------------------------------------------------------------------

// LogSlowQuery records that a database call was slowed. Called by pgxx.
func (s *Store) LogSlowQuery(statement string, took time.Duration) {
	s.emit(slog.LevelWarn, s.narrative.SlowQuery,
		slog.String("statement", statement),
		durationAttr("duration_ms", took),
		slog.Int("threshold_ms", 250),
		slog.String("db_system", "postgresql"))
}

// LogPoolExhausted records that leaked connections have starved the pool.
// Called by pgxx once the leak reaches the pool's capacity.
func (s *Store) LogPoolExhausted(leaked, poolSize int32) {
	s.emit(slog.LevelError, s.narrative.PoolExhausted,
		slog.Int("connections_in_use", int(leaked)),
		slog.Int("pool_size", int(poolSize)),
		slog.String("db_system", "postgresql"))
}

// LogConsumerStall records that a Kafka consumer has stopped progressing.
// Called by kafkax's consume loop.
func (s *Store) LogConsumerStall(topic, group string, partition int32, offset int64) {
	s.emit(slog.LevelError, s.narrative.ConsumerStall,
		slog.String("topic", topic),
		slog.String("consumer_group", group),
		slog.Int("partition", int(partition)),
		slog.Int64("stalled_at_offset", offset),
		slog.String("messaging_system", "kafka"))
}

// LogCacheBypass records that a cache read was skipped. Called by redisx.
func (s *Store) LogCacheBypass(key string) {
	s.emit(slog.LevelWarn, s.narrative.CacheBypass,
		slog.String("cache_key", key),
		slog.String("hit_ratio", pct(0)),
		slog.String("db_system", "redis"))
}

// LogDependencyTimeout records that an outbound call was cut short by the
// shortened client deadline. Called by httpx.
func (s *Store) LogDependencyTimeout(dependency string, deadline time.Duration) {
	s.emit(slog.LevelError, s.narrative.DependencyTimeout,
		slog.String("dependency", dependency),
		durationAttr("deadline_ms", deadline))
}

// SlowQuery returns how long a database call should be made to sleep.
func (s *Store) SlowQuery() time.Duration {
	return time.Duration(s.Get().SlowQueryMs) * time.Millisecond
}

// CacheDisabled reports whether cache reads should be skipped.
func (s *Store) CacheDisabled() bool { return s.Get().DisableCache }

// ConsumerStalled reports whether a Kafka consumer should stop progressing.
func (s *Store) ConsumerStalled() bool { return s.Get().ConsumerStall }

// ClientTimeout returns the outbound timeout to use, honouring the
// DependencyTimeoutMs override when set.
func (s *Store) ClientTimeout(def time.Duration) time.Duration {
	if ms := s.Get().DependencyTimeoutMs; ms > 0 {
		return time.Duration(ms) * time.Millisecond
	}
	return def
}

// LeakConn registers a release func that Clear will eventually call. While the
// DBConnLeak fault is on, the caller should never release it itself.
func (s *Store) LeakConn(release func()) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.spec.DBConnLeak {
		return false
	}
	s.leakedConns = append(s.leakedConns, release)
	return true
}

func (s *Store) latency(spec Spec) time.Duration {
	d := time.Duration(spec.LatencyMs) * time.Millisecond
	if spec.LatencyJitterMs > 0 {
		d += time.Duration(s.rollInt(spec.LatencyJitterMs)) * time.Millisecond
	}
	return d
}

func (s *Store) roll() float64 {
	s.rngMu.Lock()
	defer s.rngMu.Unlock()
	return s.rng.Float64()
}

func (s *Store) rollInt(n int) int {
	if n <= 0 {
		return 0
	}
	s.rngMu.Lock()
	defer s.rngMu.Unlock()
	return s.rng.Intn(n)
}

func burnCPU(d time.Duration) {
	deadline := time.Now().Add(d)
	x := 0
	for time.Now().Before(deadline) {
		for i := 0; i < 200000; i++ {
			x += i * i
		}
	}
	_ = x
}

func setIf[T any](dst *T, src *T) {
	if src != nil {
		*dst = *src
	}
}

// DecodePatch reads a Patch from JSON.
func DecodePatch(b []byte) (Patch, error) {
	var p Patch
	if len(b) == 0 {
		return p, nil
	}
	err := json.Unmarshal(b, &p)
	return p, err
}
