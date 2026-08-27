// Package checkout implements checkout-api, the orchestrator at the centre of
// the demo.
//
// One PlaceOrder call touches every protocol the demo exercises: HTTP to
// cart-service and shipping-quote, gRPC to pricing-engine, inventory-svc and
// payment-gw, Postgres for the order write, and a Kafka publish that starts the
// asynchronous fraud/notification chain. That makes it the service whose trace
// Causely can walk five layers deep.
package checkout

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	shopv1 "github.com/causely-oss/tracey-shop/gen/shop/v1"
	"github.com/causely-oss/tracey-shop/internal/app"
	"github.com/causely-oss/tracey-shop/internal/domain"
	"github.com/causely-oss/tracey-shop/internal/obs"
	"github.com/causely-oss/tracey-shop/internal/store"
	"github.com/causely-oss/tracey-shop/internal/transport/grpcx"
	"github.com/causely-oss/tracey-shop/internal/transport/httpx"
	"github.com/causely-oss/tracey-shop/internal/transport/kafkax"
	"github.com/causely-oss/tracey-shop/internal/transport/pgxx"
)

type server struct {
	shopv1.UnimplementedCheckoutServiceServer
	deps      *app.Deps
	pg        *pgxx.Pool
	producer  *kafkax.Producer
	cart      *httpx.Client
	shipping  *httpx.Client
	pricing   shopv1.PricingServiceClient
	inventory shopv1.InventoryServiceClient
	payment   shopv1.PaymentServiceClient

	// Order-intake backpressure. See applyBackpressure.
	lag       *kafkax.LagMonitor
	rng       *rand.Rand
	rngMu     sync.Mutex
	lastShedL atomic.Int64
}

// applyBackpressure sheds and delays order intake while the fraud-review
// backlog is deep.
//
// This is ordinary application behaviour, not an injected fault: a real
// checkout service that publishes to a review queue it cannot outrun has to
// stop accepting orders at some point. It is what turns a stalled consumer from
// a curiosity into an incident.
//
// It is also what makes Causely's own causal model come true. The model routes
// Lag.High -> Topic.Clogged -> Producers.Clogged -> Starvation ->
// producer LatencySLO/SuccessSLO, but a root cause is only classified Urgent
// when an SLO manifestation is *actually active* and at least two symptoms are
// live. Without real degradation here, the Slow Consumer root cause carries its
// static weight of 2.0 and stays Non-Urgent no matter how large the backlog is.
//
// At a clean baseline lag sits near zero, so none of this triggers and nothing
// is logged.
func (s *server) applyBackpressure(ctx context.Context) error {
	cfg := s.deps.Cfg
	if !cfg.BackpressureEnabled || s.lag == nil {
		return nil
	}

	lag, measured := s.lag.Lag()
	// Never shed on an unmeasured backlog: a monitor that cannot reach Kafka
	// must not look like a healthy consumer, nor like a stalled one.
	if !measured || lag <= int64(cfg.BackpressureLagThreshold) {
		return nil
	}

	// One line per 5s at most; the shed rate is high enough to be noisy.
	now := time.Now().Unix()
	if last := s.lastShedL.Load(); now-last >= 5 {
		if s.lastShedL.CompareAndSwap(last, now) {
			slog.Error("shedding order intake: fraud review backlog is beyond capacity",
				append(obs.LogTraceCtx(ctx),
					slog.Int64("review_backlog", lag),
					slog.Int("backlog_threshold", cfg.BackpressureLagThreshold),
					slog.String("topic", cfg.TopicOrders),
					slog.String("consumer_group", cfg.GroupFraud))...)
		}
	}

	// Queueing delay: orders that are still accepted get slower, which is what
	// pushes the latency SLO as well as the success SLO.
	if d := cfg.BackpressureLatency; d > 0 {
		select {
		case <-time.After(d):
		case <-ctx.Done():
			return status.Error(codes.DeadlineExceeded, "order intake timed out under backpressure")
		}
	}

	if s.roll() < cfg.BackpressureRejectRate {
		return status.Errorf(backpressureRejectCode,
			"order intake temporarily unavailable: fraud review backlog of %d exceeds capacity", lag)
	}
	return nil
}

// backpressureRejectCode is the gRPC code used to shed an order.
//
// It MUST be one of the codes otelgrpc maps to an Error span status —
// Unknown, DeadlineExceeded, Unimplemented, Internal, Unavailable, DataLoss
// (otelgrpc's serverStatus, which follows the OTel gRPC semantic conventions).
// Causely derives a server span's isError *solely* from the span status
// (pkg/spananalyzer/span_analyzer.go: `isError := span.StatusCode() == ERROR`),
// so a code outside that set is invisible to it.
//
// This cost a debugging cycle: ResourceExhausted is the intuitive code for load
// shedding, but it maps to Unset. checkout-api shed ~21% of orders, storefront
// returned 5xx and the load generator saw the failures — yet checkout-api's own
// error rate stayed flat in Causely, its success SLO never went at risk, and the
// Slow Consumer root cause stayed Non-Urgent.
//
// Unavailable is both in the error set and the conventional "shedding load, retry"
// code, so it is the right answer on both counts.
const backpressureRejectCode = codes.Unavailable

func (s *server) roll() float64 {
	s.rngMu.Lock()
	defer s.rngMu.Unlock()
	return s.rng.Float64()
}

// Run starts the checkout gRPC server.
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
	producer, err := d.Producer(ctx)
	if err != nil {
		return fmt.Errorf("kafka producer: %w", err)
	}
	pricing, err := d.PricingClient()
	if err != nil {
		return fmt.Errorf("pricing client: %w", err)
	}
	inventory, err := d.InventoryClient()
	if err != nil {
		return fmt.Errorf("inventory client: %w", err)
	}
	payment, err := d.PaymentClient()
	if err != nil {
		return fmt.Errorf("payment client: %w", err)
	}

	// Watch how far fraud review has fallen behind on the orders topic, so intake
	// can push back when it cannot keep up. Non-fatal: if this cannot start, the
	// service runs without backpressure rather than refusing to serve orders.
	var lagMonitor *kafkax.LagMonitor
	if d.Cfg.BackpressureEnabled {
		lagMonitor, err = kafkax.NewLagMonitor(ctx,
			d.Cfg.KafkaBrokers, d.Cfg.GroupFraud, d.Cfg.TopicOrders, d.Cfg.BackpressurePollInterval)
		if err != nil {
			slog.Warn("order-intake backpressure disabled: could not start the lag monitor",
				slog.Any("err", err))
		}
	}

	srv := grpcx.NewServer(d.Cfg.GRPCAddr, d.Faults)
	shopv1.RegisterCheckoutServiceServer(srv.Raw(), &server{
		deps:      d,
		pg:        pool,
		producer:  producer,
		cart:      d.HTTPClient(d.Cfg.CartURL),
		shipping:  d.HTTPClient(d.Cfg.ShippingURL),
		pricing:   pricing,
		inventory: inventory,
		payment:   payment,
		lag:       lagMonitor,
		rng:       rand.New(rand.NewSource(time.Now().UnixNano())),
	})

	d.Admin.SetReady(true)
	return srv.Start(ctx)
}

func (s *server) PlaceOrder(ctx context.Context, req *shopv1.PlaceOrderRequest) (*shopv1.PlaceOrderResponse, error) {
	if req.GetCartId() == "" {
		return nil, status.Error(codes.InvalidArgument, "cart id is required")
	}

	// Backpressure is applied at intake, before any downstream work: an order we
	// are going to shed should not cost the rest of the graph anything.
	if err := s.applyBackpressure(ctx); err != nil {
		return nil, err
	}

	orderID := domain.NewID("ord")
	timeout := s.deps.Cfg.RequestTimeout

	// 1. Fetch the cart over HTTP.
	var cart domain.Cart
	if err := s.cart.GetJSON(ctx, "/carts/"+req.GetCartId(), &cart); err != nil {
		return nil, fmt.Errorf("fetch cart %s: %w", req.GetCartId(), err)
	}
	if len(cart.Items) == 0 {
		// The load generator always adds an item before checking out, so an
		// empty cart is a client error rather than a service failure.
		return nil, status.Error(codes.FailedPrecondition, "cart is empty")
	}
	lineItems := domain.LineItemsToProto(cart.Items)

	// 2. Confirm stock over gRPC.
	stockCtx, cancel := grpcx.WithTimeout(ctx, s.deps.Faults, timeout)
	stock, err := s.inventory.CheckStock(stockCtx, &shopv1.CheckStockRequest{Items: lineItems})
	cancel()
	if err != nil {
		return nil, fmt.Errorf("check stock: %w", err)
	}
	if !stock.GetAllAvailable() {
		return nil, status.Errorf(codes.FailedPrecondition,
			"out of stock: %v", stock.GetUnavailableProductIds())
	}

	// 3. Price the cart over gRPC.
	quoteCtx, cancel := grpcx.WithTimeout(ctx, s.deps.Faults, timeout)
	quote, err := s.pricing.Quote(quoteCtx, &shopv1.QuoteRequest{
		CartId:       req.GetCartId(),
		Items:        lineItems,
		CustomerTier: req.GetCustomerTier(),
	})
	cancel()
	if err != nil {
		return nil, fmt.Errorf("price quote: %w", err)
	}

	// 4. Get a carrier rate over HTTP.
	var ship domain.ShippingQuoteResponse
	shipReq := domain.ShippingQuoteRequest{
		OrderID:   orderID,
		Address:   addressFromProto(req.GetShippingAddress()),
		ItemCount: totalQuantity(cart.Items),
		WeightG:   totalQuantity(cart.Items) * 450,
	}
	if err := s.shipping.PostJSON(ctx, "/quotes", shipReq, &ship); err != nil {
		return nil, fmt.Errorf("shipping quote: %w", err)
	}

	grandTotal := quote.GetTotal().GetCents() + ship.Cost.Cents

	// 5. Authorise payment over gRPC. This is the hop that continues down
	//    through ledger-svc to Postgres, making it the deepest chain.
	payCtx, cancel := grpcx.WithTimeout(ctx, s.deps.Faults, 2*timeout)
	auth, err := s.payment.Authorize(payCtx, &shopv1.AuthorizeRequest{
		OrderId:    orderID,
		CustomerId: req.GetCustomerId(),
		Amount:     &shopv1.Money{Cents: grandTotal, Currency: "USD"},
		Card:       req.GetCard(),
	})
	cancel()
	if err != nil {
		return nil, fmt.Errorf("authorize payment: %w", err)
	}

	// 6. Reserve stock and persist the order.
	resvCtx, cancel := grpcx.WithTimeout(ctx, s.deps.Faults, timeout)
	if _, err := s.inventory.ReserveStock(resvCtx, &shopv1.ReserveStockRequest{
		OrderId: orderID,
		Items:   lineItems,
	}); err != nil {
		cancel()
		return nil, fmt.Errorf("reserve stock: %w", err)
	}
	cancel()

	if err := s.persistOrder(ctx, orderID, req.GetCustomerId(), grandTotal, cart.Items); err != nil {
		return nil, err
	}

	// 7. Publish the order event, starting the async chain:
	//    orders -> fraud-detector -> risk-model -> notifications -> worker.
	//
	// Published directly on ctx, with no wrapper span. There used to be an
	// INTERNAL "publish order event" span here, and it did real damage: the
	// collector drops INTERNAL spans (Causely ignores them), which severed the
	// PRODUCER span from its parent PlaceOrder SERVER span. Causely then could not
	// resolve the producing operation and synthesised a BackgroundOperation named
	// "Background" on checkout-api instead of shop.v1.CheckoutService/PlaceOrder.
	//
	// The consequence was subtle and expensive: because the producer was a
	// synthetic background operation rather than an operation ProvidedBy the
	// checkout-api Service, the model's
	// Topic.Clogged -> Producers.Clogged -> Starvation -> ProvidedBy.SLOs chain
	// had no service SLO to reach. The Slow Consumer root cause was left with no
	// SLOs and no impacted entities in its closure, so it fell back to its static
	// weight of 2.0 and stayed Non-Urgent no matter how badly checkout degraded.
	//
	// Publish() already emits a proper PRODUCER span, so the wrapper added nothing.
	err = s.producer.Publish(ctx, s.deps.Cfg.TopicOrders, orderID, domain.OrderEvent{
		OrderID:       orderID,
		CustomerID:    req.GetCustomerId(),
		CustomerTier:  req.GetCustomerTier(),
		Email:         req.GetEmail(),
		Country:       req.GetShippingAddress().GetCountry(),
		Total:         domain.USD(grandTotal),
		Items:         cart.Items,
		TransactionID: auth.GetTransactionId(),
		PlacedAt:      time.Now().UTC(),
	})
	if err != nil {
		// The order is already paid and persisted; failing the call here would
		// misrepresent what happened. Log it and let the caller succeed.
		slog.Error("failed to publish order event",
			append(obs.LogTraceCtx(ctx), slog.String("order_id", orderID), slog.Any("err", err))...)
	}

	// 8. Clear the cart so repeated checkouts stay realistic.
	if err := s.cart.PostJSON(ctx, "/carts/"+req.GetCartId()+"/clear", nil, nil); err != nil {
		slog.Debug("cart clear failed", append(obs.LogTraceCtx(ctx), slog.Any("err", err))...)
	}

	return &shopv1.PlaceOrderResponse{
		OrderId:       orderID,
		Total:         &shopv1.Money{Cents: grandTotal, Currency: "USD"},
		ShippingCost:  ship.Cost.Proto(),
		TransactionId: auth.GetTransactionId(),
		TrackingId:    ship.TrackingID,
	}, nil
}

func (s *server) GetOrder(ctx context.Context, req *shopv1.GetOrderRequest) (*shopv1.GetOrderResponse, error) {
	if req.GetOrderId() == "" {
		return nil, status.Error(codes.InvalidArgument, "order id is required")
	}
	s.pg.SlowDown(ctx)

	var (
		orderStatus string
		cents       int64
		currency    string
	)
	err := s.pg.QueryRow(ctx,
		`SELECT status, total_cents, currency FROM orders WHERE id = $1`,
		req.GetOrderId(),
	).Scan(&orderStatus, &cents, &currency)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, status.Errorf(codes.NotFound, "order %s not found", req.GetOrderId())
	}
	if err != nil {
		return nil, fmt.Errorf("load order: %w", err)
	}

	rows, err := s.pg.Query(ctx,
		`SELECT product_id, quantity FROM order_items WHERE order_id = $1 ORDER BY product_id`,
		req.GetOrderId())
	if err != nil {
		return nil, fmt.Errorf("load order items: %w", err)
	}
	defer rows.Close()

	var items []*shopv1.LineItem
	for rows.Next() {
		var it shopv1.LineItem
		if err := rows.Scan(&it.ProductId, &it.Quantity); err != nil {
			return nil, fmt.Errorf("scan order item: %w", err)
		}
		items = append(items, &it)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate order items: %w", err)
	}

	return &shopv1.GetOrderResponse{
		OrderId: req.GetOrderId(),
		Status:  orderStatus,
		Total:   &shopv1.Money{Cents: cents, Currency: currency},
		Items:   items,
	}, nil
}

func (s *server) persistOrder(ctx context.Context, orderID, customerID string, total int64, items []domain.CartItem) error {
	tx, err := s.pg.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin order tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if _, err := tx.Exec(ctx, `
        INSERT INTO orders (id, customer_id, status, total_cents, currency)
        VALUES ($1, $2, 'confirmed', $3, 'USD')`,
		orderID, customerID, total,
	); err != nil {
		return fmt.Errorf("insert order: %w", err)
	}

	for _, item := range items {
		if _, err := tx.Exec(ctx, `
            INSERT INTO order_items (order_id, product_id, quantity)
            VALUES ($1, $2, $3)
            ON CONFLICT (order_id, product_id)
            DO UPDATE SET quantity = order_items.quantity + EXCLUDED.quantity`,
			orderID, item.ProductID, item.Quantity,
		); err != nil {
			return fmt.Errorf("insert order item: %w", err)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit order: %w", err)
	}
	return nil
}

func addressFromProto(a *shopv1.Address) domain.Address {
	if a == nil {
		return domain.Address{Country: "US"}
	}
	return domain.Address{
		Street:     a.GetStreet(),
		City:       a.GetCity(),
		Region:     a.GetRegion(),
		PostalCode: a.GetPostalCode(),
		Country:    a.GetCountry(),
	}
}

func totalQuantity(items []domain.CartItem) int32 {
	var n int32
	for _, it := range items {
		n += it.Quantity
	}
	return n
}
