// Package inventory implements inventory-svc: a gRPC service over Postgres.
//
// It is the demo's authoritative read source for the catalogue and the owner of
// stock reservations, so it carries the bulk of the Postgres query load. That
// makes it the natural place to demonstrate slow queries and connection-pool
// exhaustion.
package inventory

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	shopv1 "github.com/causely-oss/tracey-shop/gen/shop/v1"
	"github.com/causely-oss/tracey-shop/internal/app"
	"github.com/causely-oss/tracey-shop/internal/domain"
	"github.com/causely-oss/tracey-shop/internal/store"
	"github.com/causely-oss/tracey-shop/internal/transport/grpcx"
	"github.com/causely-oss/tracey-shop/internal/transport/pgxx"
)

type server struct {
	shopv1.UnimplementedInventoryServiceServer
	deps *app.Deps
	pg   *pgxx.Pool
}

// Run starts the inventory gRPC server.
func Run(ctx context.Context, d *app.Deps) error {
	pool, err := d.Postgres(ctx)
	if err != nil {
		return fmt.Errorf("postgres: %w", err)
	}
	if err := store.Migrate(ctx, pool); err != nil {
		return fmt.Errorf("migrate: %w", err)
	}
	// Self-heal if the bundled Postgres is ever recreated on an empty volume.
	store.StartReconciler(ctx, pool)
	// Keep the warehouse stocked. Without this the demo runs out of inventory
	// after a day of load and every checkout fails at the stock check — which
	// also silently breaks any scenario downstream of checkout, because those
	// requests never get that far.
	store.StartRestocker(ctx, pool)

	srv := grpcx.NewServer(d.Cfg.GRPCAddr, d.Faults)
	shopv1.RegisterInventoryServiceServer(srv.Raw(), &server{deps: d, pg: pool})

	d.Admin.SetReady(true)
	return srv.Start(ctx)
}

const listStockSQL = `
SELECT p.id, p.sku, p.name, p.category, p.price_cents, p.currency, i.available
FROM products p
JOIN inventory i ON i.product_id = p.id
WHERE ($1 = '' OR p.category = $1)
  AND ($2 = '' OR p.id = $2)
  AND ($3 = '' OR p.name ILIKE '%' || $3 || '%')
ORDER BY p.id
LIMIT $4
`

func (s *server) ListStock(ctx context.Context, req *shopv1.ListStockRequest) (*shopv1.ListStockResponse, error) {
	// Fault hooks: both are no-ops unless a scenario has enabled them.
	s.pg.SlowDown(ctx)
	s.pg.LeakConnIfEnabled(ctx)

	limit := req.GetLimit()
	if limit <= 0 || limit > 200 {
		limit = 20
	}

	rows, err := s.pg.Query(ctx, listStockSQL,
		req.GetCategory(), req.GetProductId(), req.GetQuery(), limit)
	if err != nil {
		return nil, fmt.Errorf("query stock: %w", err)
	}
	defer rows.Close()

	var products []*shopv1.Product
	for rows.Next() {
		var (
			p         shopv1.Product
			cents     int64
			currency  string
			available int32
		)
		if err := rows.Scan(&p.Id, &p.Sku, &p.Name, &p.Category, &cents, &currency, &available); err != nil {
			return nil, fmt.Errorf("scan product: %w", err)
		}
		p.Price = &shopv1.Money{Cents: cents, Currency: currency}
		p.Available = available
		products = append(products, &p)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate products: %w", err)
	}

	return &shopv1.ListStockResponse{Products: products}, nil
}

func (s *server) CheckStock(ctx context.Context, req *shopv1.CheckStockRequest) (*shopv1.CheckStockResponse, error) {
	if len(req.GetItems()) == 0 {
		return &shopv1.CheckStockResponse{AllAvailable: true}, nil
	}
	s.pg.SlowDown(ctx)

	var unavailable []string
	for _, item := range req.GetItems() {
		var available int32
		err := s.pg.QueryRow(ctx,
			`SELECT available FROM inventory WHERE product_id = $1`,
			item.GetProductId(),
		).Scan(&available)
		if errors.Is(err, pgx.ErrNoRows) {
			unavailable = append(unavailable, item.GetProductId())
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("check stock for %s: %w", item.GetProductId(), err)
		}
		if available < item.GetQuantity() {
			unavailable = append(unavailable, item.GetProductId())
		}
	}

	return &shopv1.CheckStockResponse{
		AllAvailable:          len(unavailable) == 0,
		UnavailableProductIds: unavailable,
	}, nil
}

func (s *server) ReserveStock(ctx context.Context, req *shopv1.ReserveStockRequest) (*shopv1.ReserveStockResponse, error) {
	if req.GetOrderId() == "" {
		return nil, status.Error(codes.InvalidArgument, "order id is required")
	}
	s.pg.SlowDown(ctx)

	reservationID := domain.NewID("resv")

	tx, err := s.pg.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin reservation tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	for _, item := range req.GetItems() {
		// The seeded stock is large enough that this never underflows on the
		// happy path, and the GREATEST guard keeps it from doing so if it ever
		// does — a clean baseline must not produce errors.
		if _, err := tx.Exec(ctx, `
            UPDATE inventory
            SET available = GREATEST(available - $2, 0),
                reserved  = reserved + LEAST($2, available)
            WHERE product_id = $1`,
			item.GetProductId(), item.GetQuantity(),
		); err != nil {
			return nil, fmt.Errorf("reserve %s: %w", item.GetProductId(), err)
		}
	}

	if _, err := tx.Exec(ctx,
		`INSERT INTO stock_reservations (id, order_id) VALUES ($1, $2)`,
		reservationID, req.GetOrderId(),
	); err != nil {
		return nil, fmt.Errorf("record reservation: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit reservation: %w", err)
	}

	return &shopv1.ReserveStockResponse{ReservationId: reservationID}, nil
}
