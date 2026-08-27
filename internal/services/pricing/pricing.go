// Package pricing implements pricing-engine: a gRPC service that reads discount
// rules from Postgres, caches them in Valkey, and prices a cart.
//
// It depends on both backends but on no other service, so it is a clean leaf for
// demonstrating a CPU-bound root cause that propagates up through checkout to
// the storefront without any error at all.
package pricing

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	shopv1 "github.com/causely-oss/tracey-shop/gen/shop/v1"
	"github.com/causely-oss/tracey-shop/internal/app"
	"github.com/causely-oss/tracey-shop/internal/obs"
	"github.com/causely-oss/tracey-shop/internal/store"
	"github.com/causely-oss/tracey-shop/internal/transport/grpcx"
	"github.com/causely-oss/tracey-shop/internal/transport/pgxx"
	"github.com/causely-oss/tracey-shop/internal/transport/redisx"
)

// taxBps is a flat 8.25% sales tax, applied after discounts.
const taxBps = 825

const ruleCacheTTL = 60 * time.Second

type server struct {
	shopv1.UnimplementedPricingServiceServer
	deps  *app.Deps
	pg    *pgxx.Pool
	cache *redisx.Client
}

// Run starts the pricing gRPC server.
func Run(ctx context.Context, d *app.Deps) error {
	pool, err := d.Postgres(ctx)
	if err != nil {
		return fmt.Errorf("postgres: %w", err)
	}
	cache, err := d.Redis(ctx)
	if err != nil {
		return fmt.Errorf("redis: %w", err)
	}
	// pricing-engine reads products and price_rules, so it needs the schema to
	// exist — and to be reapplied if the bundled Postgres is ever recreated on an
	// empty volume.
	if err := store.Migrate(ctx, pool); err != nil {
		return fmt.Errorf("migrate: %w", err)
	}
	store.StartReconciler(ctx, pool)

	srv := grpcx.NewServer(d.Cfg.GRPCAddr, d.Faults)
	shopv1.RegisterPricingServiceServer(srv.Raw(), &server{deps: d, pg: pool, cache: cache})

	d.Admin.SetReady(true)
	return srv.Start(ctx)
}

// rule is a cached discount rule.
type rule struct {
	Category    string `json:"category"`
	DiscountBps int    `json:"discountBps"`
	Description string `json:"description"`
}

type ruleSet struct {
	Rules []rule `json:"rules"`
}

func (s *server) Quote(ctx context.Context, req *shopv1.QuoteRequest) (*shopv1.QuoteResponse, error) {
	if len(req.GetItems()) == 0 {
		return nil, status.Error(codes.InvalidArgument, "at least one line item is required")
	}
	tier := req.GetCustomerTier()
	if tier == "" {
		tier = "standard"
	}

	rules, err := s.loadRules(ctx, tier)
	if err != nil {
		return nil, err
	}
	discountByCategory := make(map[string]rule, len(rules))
	for _, r := range rules {
		discountByCategory[r.Category] = r
	}

	// Priced from the products table so the quote reflects authoritative prices
	// rather than whatever the client claimed.
	s.pg.SlowDown(ctx)

	var (
		subtotal int64
		discount int64
		applied  []string
		seen     = make(map[string]bool)
	)

	for _, item := range req.GetItems() {
		var (
			cents    int64
			category string
		)
		err := s.pg.QueryRow(ctx,
			`SELECT price_cents, category FROM products WHERE id = $1`,
			item.GetProductId(),
		).Scan(&cents, &category)
		if err != nil {
			return nil, fmt.Errorf("price lookup for %s: %w", item.GetProductId(), err)
		}

		line := cents * int64(item.GetQuantity())
		subtotal += line

		if r, ok := discountByCategory[category]; ok && r.DiscountBps > 0 {
			discount += line * int64(r.DiscountBps) / 10000
			if !seen[r.Description] {
				applied = append(applied, r.Description)
				seen[r.Description] = true
			}
		}
	}

	taxable := subtotal - discount
	tax := taxable * taxBps / 10000
	total := taxable + tax

	return &shopv1.QuoteResponse{
		Subtotal:     &shopv1.Money{Cents: subtotal, Currency: "USD"},
		Discount:     &shopv1.Money{Cents: discount, Currency: "USD"},
		Tax:          &shopv1.Money{Cents: tax, Currency: "USD"},
		Total:        &shopv1.Money{Cents: total, Currency: "USD"},
		AppliedRules: applied,
	}, nil
}

// loadRules returns the active rules for a tier, reading through Valkey.
func (s *server) loadRules(ctx context.Context, tier string) ([]rule, error) {
	key := "pricing:rules:" + tier

	var cached ruleSet
	found, err := s.cache.GetJSON(ctx, key, &cached)
	if err != nil {
		slog.Warn("rule cache read failed", append(obs.LogTraceCtx(ctx), slog.Any("err", err))...)
	}
	if found && len(cached.Rules) > 0 {
		return cached.Rules, nil
	}

	rows, err := s.pg.Query(ctx, `
        SELECT category, discount_bps, description
        FROM price_rules
        WHERE customer_tier = $1 AND active
        ORDER BY category`, tier)
	if err != nil {
		return nil, fmt.Errorf("query price rules: %w", err)
	}
	defer rows.Close()

	var rules []rule
	for rows.Next() {
		var r rule
		if err := rows.Scan(&r.Category, &r.DiscountBps, &r.Description); err != nil {
			return nil, fmt.Errorf("scan price rule: %w", err)
		}
		rules = append(rules, r)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate price rules: %w", err)
	}

	if err := s.cache.SetJSON(ctx, key, ruleSet{Rules: rules}, ruleCacheTTL); err != nil {
		slog.Warn("rule cache write failed", append(obs.LogTraceCtx(ctx), slog.Any("err", err))...)
	}
	return rules, nil
}
