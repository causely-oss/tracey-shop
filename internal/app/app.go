// Package app holds the shared runtime wiring every role receives.
//
// Backends and downstream clients are created lazily, so a role only connects to
// what it actually uses. That keeps the topology honest: a service shows up as
// depending on Postgres only if it really queries Postgres.
package app

import (
	"context"
	"fmt"
	"sync"

	"google.golang.org/grpc"

	shopv1 "github.com/causely-oss/tracey-shop/gen/shop/v1"
	"github.com/causely-oss/tracey-shop/internal/admin"
	"github.com/causely-oss/tracey-shop/internal/config"
	"github.com/causely-oss/tracey-shop/internal/faults"
	"github.com/causely-oss/tracey-shop/internal/transport/grpcx"
	"github.com/causely-oss/tracey-shop/internal/transport/httpx"
	"github.com/causely-oss/tracey-shop/internal/transport/kafkax"
	"github.com/causely-oss/tracey-shop/internal/transport/pgxx"
	"github.com/causely-oss/tracey-shop/internal/transport/redisx"
)

// RunFunc is the entry point every role implements.
type RunFunc func(ctx context.Context, d *Deps) error

// Deps is the shared plumbing handed to a role.
type Deps struct {
	Cfg    *config.Config
	Faults *faults.Store
	Admin  *admin.Server

	mu       sync.Mutex
	pg       *pgxx.Pool
	rdb      *redisx.Client
	producer *kafkax.Producer
	conns    []*grpc.ClientConn
	closers  []func() error
}

// New builds the shared dependencies for a pod.
func New(cfg *config.Config, store *faults.Store, adminSrv *admin.Server) *Deps {
	return &Deps{Cfg: cfg, Faults: store, Admin: adminSrv}
}

// Postgres returns the shared traced pool, opening it on first use.
func (d *Deps) Postgres(ctx context.Context) (*pgxx.Pool, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.pg != nil {
		return d.pg, nil
	}
	if d.Cfg.PostgresDSN == "" {
		return nil, fmt.Errorf("POSTGRES_DSN is not set")
	}
	pool, err := pgxx.New(ctx, d.Cfg.PostgresDSN, d.Faults)
	if err != nil {
		return nil, err
	}
	d.pg = pool
	d.closers = append(d.closers, func() error { pool.Close(); return nil })
	return pool, nil
}

// Redis returns the shared traced cache client, opening it on first use.
func (d *Deps) Redis(ctx context.Context) (*redisx.Client, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.rdb != nil {
		return d.rdb, nil
	}
	if d.Cfg.RedisAddr == "" {
		return nil, fmt.Errorf("REDIS_ADDR is not set")
	}
	c, err := redisx.New(ctx, d.Cfg.RedisAddr, d.Cfg.RedisDB, d.Faults)
	if err != nil {
		return nil, err
	}
	d.rdb = c
	d.closers = append(d.closers, c.Close)
	return c, nil
}

// Producer returns the shared Kafka producer, connecting on first use.
func (d *Deps) Producer(ctx context.Context) (*kafkax.Producer, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.producer != nil {
		return d.producer, nil
	}
	if len(d.Cfg.KafkaBrokers) == 0 {
		return nil, fmt.Errorf("KAFKA_BROKERS is not set")
	}
	p, err := kafkax.NewProducer(ctx, d.Cfg.KafkaBrokers)
	if err != nil {
		return nil, err
	}
	d.producer = p
	d.closers = append(d.closers, p.Close)
	return p, nil
}

// HTTPClient builds an instrumented client for a downstream base URL.
func (d *Deps) HTTPClient(baseURL string) *httpx.Client {
	return httpx.NewClient(baseURL, d.Cfg.RequestTimeout, d.Faults)
}

func (d *Deps) dial(target string) (*grpc.ClientConn, error) {
	conn, err := grpcx.Dial(target)
	if err != nil {
		return nil, err
	}
	d.mu.Lock()
	d.conns = append(d.conns, conn)
	d.closers = append(d.closers, conn.Close)
	d.mu.Unlock()
	return conn, nil
}

// CatalogClient dials catalog-api.
func (d *Deps) CatalogClient() (shopv1.CatalogServiceClient, error) {
	conn, err := d.dial(d.Cfg.CatalogAddr)
	if err != nil {
		return nil, err
	}
	return shopv1.NewCatalogServiceClient(conn), nil
}

// CheckoutClient dials checkout-api.
func (d *Deps) CheckoutClient() (shopv1.CheckoutServiceClient, error) {
	conn, err := d.dial(d.Cfg.CheckoutAddr)
	if err != nil {
		return nil, err
	}
	return shopv1.NewCheckoutServiceClient(conn), nil
}

// PricingClient dials pricing-engine.
func (d *Deps) PricingClient() (shopv1.PricingServiceClient, error) {
	conn, err := d.dial(d.Cfg.PricingAddr)
	if err != nil {
		return nil, err
	}
	return shopv1.NewPricingServiceClient(conn), nil
}

// InventoryClient dials inventory-svc.
func (d *Deps) InventoryClient() (shopv1.InventoryServiceClient, error) {
	conn, err := d.dial(d.Cfg.InventoryAddr)
	if err != nil {
		return nil, err
	}
	return shopv1.NewInventoryServiceClient(conn), nil
}

// PaymentClient dials payment-gw.
func (d *Deps) PaymentClient() (shopv1.PaymentServiceClient, error) {
	conn, err := d.dial(d.Cfg.PaymentAddr)
	if err != nil {
		return nil, err
	}
	return shopv1.NewPaymentServiceClient(conn), nil
}

// LedgerClient dials ledger-svc.
func (d *Deps) LedgerClient() (shopv1.LedgerServiceClient, error) {
	conn, err := d.dial(d.Cfg.LedgerAddr)
	if err != nil {
		return nil, err
	}
	return shopv1.NewLedgerServiceClient(conn), nil
}

// RiskClient dials risk-model.
func (d *Deps) RiskClient() (shopv1.RiskServiceClient, error) {
	conn, err := d.dial(d.Cfg.RiskAddr)
	if err != nil {
		return nil, err
	}
	return shopv1.NewRiskServiceClient(conn), nil
}

// Close releases every resource opened through Deps.
func (d *Deps) Close() {
	d.mu.Lock()
	closers := d.closers
	d.closers = nil
	d.mu.Unlock()

	// Reverse order, so clients close before the pools they were built on.
	for i := len(closers) - 1; i >= 0; i-- {
		_ = closers[i]()
	}
}
