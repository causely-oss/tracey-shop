// Package risk implements risk-model: the deepest service on the asynchronous
// branch, scoring orders against a Valkey feature store.
//
// Reaching it takes a Kafka hop plus a gRPC hop from checkout, so it is the test
// of whether trace context survived the message queue: if header propagation
// broke, risk-model would appear as an orphaned root in Causely instead of a
// descendant of the checkout trace.
package risk

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	shopv1 "github.com/causely-oss/tracey-shop/gen/shop/v1"
	"github.com/causely-oss/tracey-shop/internal/app"
	"github.com/causely-oss/tracey-shop/internal/domain"
	"github.com/causely-oss/tracey-shop/internal/transport/grpcx"
	"github.com/causely-oss/tracey-shop/internal/transport/redisx"
)

// featureTTL keeps the per-customer feature window bounded.
const featureTTL = 24 * time.Hour

// Decision thresholds, chosen so the demo's traffic is overwhelmingly approved.
const (
	reviewThreshold  = 0.70
	declineThreshold = 0.90
)

type server struct {
	shopv1.UnimplementedRiskServiceServer
	deps  *app.Deps
	store *redisx.Client
}

// Run starts the risk gRPC server.
func Run(ctx context.Context, d *app.Deps) error {
	features, err := d.Redis(ctx)
	if err != nil {
		return fmt.Errorf("redis: %w", err)
	}

	srv := grpcx.NewServer(d.Cfg.GRPCAddr, d.Faults)
	shopv1.RegisterRiskServiceServer(srv.Raw(), &server{deps: d, store: features})

	d.Admin.SetReady(true)
	return srv.Start(ctx)
}

func (s *server) Score(ctx context.Context, req *shopv1.ScoreRequest) (*shopv1.ScoreResponse, error) {
	if req.GetOrderId() == "" {
		return nil, status.Error(codes.InvalidArgument, "order id is required")
	}

	customer := req.GetCustomerId()
	if customer == "" {
		customer = "anonymous"
	}
	amount := domain.MoneyFromProto(req.GetAmount())

	// Feature 1: how many orders this customer has placed recently.
	orderCount, err := s.store.IncrBy(ctx, "risk:orders:"+customer, 1, featureTTL)
	if err != nil {
		return nil, fmt.Errorf("read order-count feature: %w", err)
	}

	// Feature 2: rolling spend for the customer.
	spend, err := s.store.IncrBy(ctx, "risk:spend:"+customer, amount.Cents, featureTTL)
	if err != nil {
		return nil, fmt.Errorf("read spend feature: %w", err)
	}

	// Feature 3: a per-country prior, seeded on first sight.
	countryKey := "risk:country:" + nonEmpty(req.GetCountry(), "US")
	priors, err := s.store.HGetAllInt(ctx, countryKey)
	if err != nil {
		return nil, fmt.Errorf("read country prior: %w", err)
	}
	prior := 0.05
	if raw, ok := priors["prior"]; ok {
		if v, err := strconv.ParseFloat(raw, 64); err == nil {
			prior = v
		}
	} else if err := s.store.HSet(ctx, countryKey, "prior", "0.05"); err != nil {
		return nil, fmt.Errorf("seed country prior: %w", err)
	}

	// A small, bounded, deterministic model. Values are tuned so ordinary demo
	// traffic scores well below the review threshold: the point of a clean
	// baseline is that nothing looks anomalous until a fault is injected.
	score := prior
	score += amountRisk(amount.Cents)
	score += velocityRisk(orderCount)
	score += spendRisk(spend)
	if score > 1 {
		score = 1
	}

	var signals []string
	if amount.Cents > 50_000 {
		signals = append(signals, "high_order_value")
	}
	if orderCount > 50 {
		signals = append(signals, "high_order_velocity")
	}
	if req.GetCountry() != "" && req.GetCountry() != "US" {
		signals = append(signals, "cross_border")
	}

	decision := "approve"
	switch {
	case score >= declineThreshold:
		decision = "decline"
	case score >= reviewThreshold:
		decision = "review"
	}

	return &shopv1.ScoreResponse{
		Score:    score,
		Decision: decision,
		Signals:  signals,
	}, nil
}

// amountRisk contributes at most 0.20, saturating at $1,000.
func amountRisk(cents int64) float64 {
	r := float64(cents) / 100_000.0 * 0.20
	return min(r, 0.20)
}

// velocityRisk contributes at most 0.15.
func velocityRisk(orders int64) float64 {
	r := float64(orders) / 200.0 * 0.15
	return min(r, 0.15)
}

// spendRisk contributes at most 0.10.
func spendRisk(cents int64) float64 {
	r := float64(cents) / 5_000_000.0 * 0.10
	return min(r, 0.10)
}

func nonEmpty(v, def string) string {
	if v == "" {
		return def
	}
	return v
}
