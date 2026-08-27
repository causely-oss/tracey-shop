// Package catalog implements catalog-api: a gRPC read service in front of
// inventory-svc, with a Valkey cache.
//
// The cache gives the demo a read path whose load profile can be shifted onto
// Postgres by flipping the disableCache fault, which is a distinct failure shape
// from an error rate or a latency spike.
package catalog

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	shopv1 "github.com/causely-oss/tracey-shop/gen/shop/v1"
	"github.com/causely-oss/tracey-shop/internal/app"
	"github.com/causely-oss/tracey-shop/internal/obs"
	"github.com/causely-oss/tracey-shop/internal/transport/grpcx"
	"github.com/causely-oss/tracey-shop/internal/transport/redisx"
)

const cacheTTL = 30 * time.Second

type server struct {
	shopv1.UnimplementedCatalogServiceServer
	deps      *app.Deps
	cache     *redisx.Client
	inventory shopv1.InventoryServiceClient
}

// Run starts the catalog gRPC server.
func Run(ctx context.Context, d *app.Deps) error {
	cache, err := d.Redis(ctx)
	if err != nil {
		return fmt.Errorf("redis: %w", err)
	}
	inventory, err := d.InventoryClient()
	if err != nil {
		return fmt.Errorf("inventory client: %w", err)
	}

	srv := grpcx.NewServer(d.Cfg.GRPCAddr, d.Faults)
	shopv1.RegisterCatalogServiceServer(srv.Raw(), &server{
		deps:      d,
		cache:     cache,
		inventory: inventory,
	})

	d.Admin.SetReady(true)
	return srv.Start(ctx)
}

func (s *server) ListProducts(ctx context.Context, req *shopv1.ListProductsRequest) (*shopv1.ListProductsResponse, error) {
	limit := clampLimit(req.GetLimit())
	key := fmt.Sprintf("catalog:list:%s:%d", req.GetCategory(), limit)

	if products, hit := s.readCache(ctx, key); hit {
		return &shopv1.ListProductsResponse{Products: products, CacheHit: true}, nil
	}

	ctx, cancel := grpcx.WithTimeout(ctx, s.deps.Faults, s.deps.Cfg.RequestTimeout)
	defer cancel()

	resp, err := s.inventory.ListStock(ctx, &shopv1.ListStockRequest{
		Category: req.GetCategory(),
		Limit:    limit,
	})
	if err != nil {
		return nil, fmt.Errorf("list stock: %w", err)
	}

	s.writeCache(ctx, key, resp.GetProducts())
	return &shopv1.ListProductsResponse{Products: resp.GetProducts(), CacheHit: false}, nil
}

func (s *server) GetProduct(ctx context.Context, req *shopv1.GetProductRequest) (*shopv1.GetProductResponse, error) {
	if req.GetId() == "" {
		return nil, status.Error(codes.InvalidArgument, "product id is required")
	}
	key := "catalog:product:" + req.GetId()

	if products, hit := s.readCache(ctx, key); hit && len(products) > 0 {
		return &shopv1.GetProductResponse{Product: products[0], CacheHit: true}, nil
	}

	ctx, cancel := grpcx.WithTimeout(ctx, s.deps.Faults, s.deps.Cfg.RequestTimeout)
	defer cancel()

	resp, err := s.inventory.ListStock(ctx, &shopv1.ListStockRequest{
		ProductId: req.GetId(),
		Limit:     1,
	})
	if err != nil {
		return nil, fmt.Errorf("list stock for %s: %w", req.GetId(), err)
	}
	if len(resp.GetProducts()) == 0 {
		return nil, status.Errorf(codes.NotFound, "product %s not found", req.GetId())
	}

	s.writeCache(ctx, key, resp.GetProducts()[:1])
	return &shopv1.GetProductResponse{Product: resp.GetProducts()[0], CacheHit: false}, nil
}

func (s *server) SearchProducts(ctx context.Context, req *shopv1.SearchProductsRequest) (*shopv1.ListProductsResponse, error) {
	query := strings.TrimSpace(req.GetQuery())
	if query == "" {
		return nil, status.Error(codes.InvalidArgument, "query is required")
	}
	limit := clampLimit(req.GetLimit())

	// Searches deliberately bypass the cache: it keeps a steady stream of read
	// traffic flowing through to Postgres so the DB dependency stays visible in
	// the topology even when the cache is warm.
	ctx, cancel := grpcx.WithTimeout(ctx, s.deps.Faults, s.deps.Cfg.RequestTimeout)
	defer cancel()

	resp, err := s.inventory.ListStock(ctx, &shopv1.ListStockRequest{
		Query: query,
		Limit: limit,
	})
	if err != nil {
		return nil, fmt.Errorf("search stock: %w", err)
	}
	return &shopv1.ListProductsResponse{Products: resp.GetProducts()}, nil
}

func (s *server) readCache(ctx context.Context, key string) ([]*shopv1.Product, bool) {
	var entry cacheEntry
	found, err := s.cache.GetJSON(ctx, key, &entry)
	if err != nil {
		// A cache read failure must not fail the request: fall through to the
		// authoritative source.
		slog.Warn("cache read failed", append(obs.LogTraceCtx(ctx), slog.Any("err", err))...)
		return nil, false
	}
	if !found {
		return nil, false
	}
	return entry.toProto(), true
}

func (s *server) writeCache(ctx context.Context, key string, products []*shopv1.Product) {
	if err := s.cache.SetJSON(ctx, key, newCacheEntry(products), cacheTTL); err != nil {
		slog.Warn("cache write failed", append(obs.LogTraceCtx(ctx), slog.Any("err", err))...)
	}
}

// cacheEntry is a JSON-friendly mirror of the protobuf product list.
type cacheEntry struct {
	Products []cachedProduct `json:"products"`
}

type cachedProduct struct {
	ID        string `json:"id"`
	SKU       string `json:"sku"`
	Name      string `json:"name"`
	Category  string `json:"category"`
	Cents     int64  `json:"cents"`
	Currency  string `json:"currency"`
	Available int32  `json:"available"`
}

func newCacheEntry(in []*shopv1.Product) cacheEntry {
	out := cacheEntry{Products: make([]cachedProduct, 0, len(in))}
	for _, p := range in {
		out.Products = append(out.Products, cachedProduct{
			ID:        p.GetId(),
			SKU:       p.GetSku(),
			Name:      p.GetName(),
			Category:  p.GetCategory(),
			Cents:     p.GetPrice().GetCents(),
			Currency:  p.GetPrice().GetCurrency(),
			Available: p.GetAvailable(),
		})
	}
	return out
}

func (c cacheEntry) toProto() []*shopv1.Product {
	out := make([]*shopv1.Product, 0, len(c.Products))
	for _, p := range c.Products {
		out = append(out, &shopv1.Product{
			Id:        p.ID,
			Sku:       p.SKU,
			Name:      p.Name,
			Category:  p.Category,
			Price:     &shopv1.Money{Cents: p.Cents, Currency: p.Currency},
			Available: p.Available,
		})
	}
	return out
}

func clampLimit(limit int32) int32 {
	if limit <= 0 {
		return 20
	}
	if limit > 200 {
		return 200
	}
	return limit
}
