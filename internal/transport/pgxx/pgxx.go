// Package pgxx provides the demo's traced Postgres pool.
//
// otelpgx emits everything Causely needs to model a database dependency:
// db.system=postgresql, db.query.text, db.namespace, server.address and
// server.port. It resolves the Postgres Service and attributes queries — and
// therefore slow queries — to it.
package pgxx

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/exaring/otelpgx"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.opentelemetry.io/otel/attribute"
	semconv "go.opentelemetry.io/otel/semconv/v1.34.0"

	"github.com/causely-oss/tracey-shop/internal/faults"
)

// Pool wraps a pgxpool with fault-aware helpers.
type Pool struct {
	*pgxpool.Pool
	faults *faults.Store
}

// New opens a traced connection pool and waits for Postgres to accept queries.
func New(ctx context.Context, dsn string, store *faults.Store) (*Pool, error) {
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("parse postgres dsn: %w", err)
	}

	// db.namespace is asserted explicitly rather than relying on otelpgx to emit
	// it. Causely names the Database entity from db.namespace (falling back to
	// db.name); without either it creates one called "unknown", which then sits
	// in the topology alongside the properly-named entity the PostgreSQL scraper
	// produces. Verified on a live cluster: db.namespace was absent from the
	// spans reaching the collector, and Causely had two Database entities for one
	// database.
	//
	// db.name is the pre-1.27 spelling, included for the same reason the Kafka
	// producer sets attributes by hand: readers pinned to an older semconv
	// vintage look for a different key.
	cfg.ConnConfig.Tracer = otelpgx.NewTracer(
		otelpgx.WithTrimSQLInSpanName(),
		otelpgx.WithAttributes(
			semconv.DBNamespace(cfg.ConnConfig.Database),
			attribute.String("db.name", cfg.ConnConfig.Database),
		),
	)
	cfg.MaxConns = 12
	cfg.MinConns = 2
	cfg.MaxConnLifetime = 30 * time.Minute
	cfg.MaxConnIdleTime = 5 * time.Minute
	cfg.HealthCheckPeriod = 30 * time.Second

	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("create postgres pool: %w", err)
	}

	if err := waitReady(ctx, pool); err != nil {
		pool.Close()
		return nil, err
	}

	slog.Info("postgres pool ready",
		slog.String("host", cfg.ConnConfig.Host),
		slog.String("database", cfg.ConnConfig.Database))

	return &Pool{Pool: pool, faults: store}, nil
}

func waitReady(ctx context.Context, pool *pgxpool.Pool) error {
	deadline := time.Now().Add(2 * time.Minute)
	var lastErr error
	for time.Now().Before(deadline) {
		pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		err := pool.Ping(pingCtx)
		cancel()
		if err == nil {
			return nil
		}
		lastErr = err
		slog.Info("waiting for postgres", slog.Any("err", err))
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(3 * time.Second):
		}
	}
	return fmt.Errorf("postgres not ready: %w", lastErr)
}

// SlowDown injects a server-side sleep before a query when the slowQueryMs
// fault is active. Sleeping inside Postgres — rather than in Go — is what makes
// the delay visible as database time in Causely's slow-query view instead of
// looking like application latency.
func (p *Pool) SlowDown(ctx context.Context) {
	d := p.faults.SlowQuery()
	if d <= 0 {
		return
	}
	seconds := d.Seconds()
	started := time.Now()
	if _, err := p.Pool.Exec(ctx, "SELECT pg_sleep($1)", seconds); err != nil {
		slog.Debug("pg_sleep failed", slog.Any("err", err))
		return
	}
	// Report it the way a real slow-query log would: the statement and how long
	// it actually took. This is what Causely reads as evidence for the latency.
	p.faults.LogSlowQuery("SELECT pg_sleep($1)", time.Since(started))
}

// LeakConnIfEnabled acquires and deliberately never releases a pool connection
// while the dbConnLeak fault is on, driving the pool to exhaustion.
func (p *Pool) LeakConnIfEnabled(ctx context.Context) {
	if !p.faults.Get().DBConnLeak {
		return
	}
	conn, err := p.Pool.Acquire(ctx)
	if err != nil {
		// Acquire failing IS the exhaustion; report it with the pool's own
		// numbers so Causely can name connection starvation as the cause.
		stat := p.Pool.Stat()
		p.faults.LogPoolExhausted(stat.AcquiredConns(), stat.MaxConns())
		return
	}
	if !p.faults.LeakConn(conn.Release) {
		conn.Release()
		return
	}

	// Warn as the pool fills, not only once it is empty, so the log shows the
	// trend rather than just the cliff.
	stat := p.Pool.Stat()
	if stat.AcquiredConns() >= stat.MaxConns()-1 {
		p.faults.LogPoolExhausted(stat.AcquiredConns(), stat.MaxConns())
	}
}
