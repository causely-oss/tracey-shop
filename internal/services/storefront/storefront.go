// Package storefront implements storefront-bff, the demo's HTTP edge service.
//
// It is the only service the load generator talks to. It fans out over gRPC to
// catalog-api and checkout-api and over HTTP to cart-service, which gives
// Causely a single entry point whose error rate and latency reflect the health
// of everything beneath it.
package storefront

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"

	shopv1 "github.com/causely-oss/tracey-shop/gen/shop/v1"
	"github.com/causely-oss/tracey-shop/internal/app"
	"github.com/causely-oss/tracey-shop/internal/domain"
	"github.com/causely-oss/tracey-shop/internal/transport/grpcx"
	"github.com/causely-oss/tracey-shop/internal/transport/httpx"
)

// Run starts the storefront HTTP server.
func Run(ctx context.Context, d *app.Deps) error {
	catalog, err := d.CatalogClient()
	if err != nil {
		return fmt.Errorf("catalog client: %w", err)
	}
	checkout, err := d.CheckoutClient()
	if err != nil {
		return fmt.Errorf("checkout client: %w", err)
	}
	cart := d.HTTPClient(d.Cfg.CartURL)

	s := httpx.NewServer(d.Cfg.ServiceName, d.Cfg.HTTPAddr, d.Faults)

	s.Route("GET /api/products", func(ctx context.Context, r *http.Request) (any, error) {
		ctx, cancel := grpcx.WithTimeout(ctx, d.Faults, d.Cfg.RequestTimeout)
		defer cancel()

		resp, err := catalog.ListProducts(ctx, &shopv1.ListProductsRequest{
			Category: r.URL.Query().Get("category"),
			Limit:    intParam(r, "limit", 20),
		})
		if err != nil {
			return nil, fmt.Errorf("list products: %w", err)
		}
		return map[string]any{
			"products": domain.ProductsFromProto(resp.GetProducts()),
			"cacheHit": resp.GetCacheHit(),
		}, nil
	})

	s.Route("GET /api/products/{id}", func(ctx context.Context, r *http.Request) (any, error) {
		id := r.PathValue("id")
		if id == "" {
			return nil, &httpx.BadRequestError{Msg: "product id is required"}
		}

		ctx, cancel := grpcx.WithTimeout(ctx, d.Faults, d.Cfg.RequestTimeout)
		defer cancel()

		resp, err := catalog.GetProduct(ctx, &shopv1.GetProductRequest{Id: id})
		if err != nil {
			return nil, fmt.Errorf("get product %s: %w", id, err)
		}
		if resp.GetProduct() == nil {
			return nil, &httpx.NotFoundError{Msg: "product " + id + " not found"}
		}
		return map[string]any{
			"product":  domain.ProductFromProto(resp.GetProduct()),
			"cacheHit": resp.GetCacheHit(),
		}, nil
	})

	s.Route("GET /api/search", func(ctx context.Context, r *http.Request) (any, error) {
		q := r.URL.Query().Get("q")
		if q == "" {
			return nil, &httpx.BadRequestError{Msg: "q is required"}
		}

		ctx, cancel := grpcx.WithTimeout(ctx, d.Faults, d.Cfg.RequestTimeout)
		defer cancel()

		resp, err := catalog.SearchProducts(ctx, &shopv1.SearchProductsRequest{
			Query: q,
			Limit: intParam(r, "limit", 20),
		})
		if err != nil {
			return nil, fmt.Errorf("search products: %w", err)
		}
		return map[string]any{
			"query":    q,
			"products": domain.ProductsFromProto(resp.GetProducts()),
		}, nil
	})

	s.Route("GET /api/cart/{id}", func(ctx context.Context, r *http.Request) (any, error) {
		var out domain.Cart
		if err := cart.GetJSON(ctx, "/carts/"+r.PathValue("id"), &out); err != nil {
			return nil, fmt.Errorf("fetch cart: %w", err)
		}
		return out, nil
	})

	s.Route("POST /api/cart/{id}/items", func(ctx context.Context, r *http.Request) (any, error) {
		var in domain.AddToCartRequest
		if err := httpx.DecodeJSON(r, &in); err != nil {
			return nil, err
		}
		if in.Quantity <= 0 {
			in.Quantity = 1
		}

		var out domain.Cart
		if err := cart.PostJSON(ctx, "/carts/"+r.PathValue("id")+"/items", in, &out); err != nil {
			return nil, fmt.Errorf("add to cart: %w", err)
		}
		return out, nil
	})

	// The browser UI needs an "empty cart" action. cart-service already has the
	// endpoint; the BFF just never exposed it, because the load generator only
	// ever adds items.
	s.Route("POST /api/cart/{id}/clear", func(ctx context.Context, r *http.Request) (any, error) {
		id := r.PathValue("id")
		if id == "" {
			return nil, &httpx.BadRequestError{Msg: "cart id is required"}
		}

		var out map[string]string
		if err := cart.PostJSON(ctx, "/carts/"+id+"/clear", nil, &out); err != nil {
			return nil, fmt.Errorf("clear cart: %w", err)
		}
		return out, nil
	})

	s.Route("POST /api/checkout", func(ctx context.Context, r *http.Request) (any, error) {
		var in domain.CheckoutRequest
		if err := httpx.DecodeJSON(r, &in); err != nil {
			return nil, err
		}
		if in.CartID == "" {
			return nil, &httpx.BadRequestError{Msg: "cartId is required"}
		}
		if in.CustomerTier == "" {
			in.CustomerTier = "standard"
		}

		// Checkout fans out over four services and a Kafka publish, so it gets
		// a longer budget than a catalogue read.
		ctx, cancel := grpcx.WithTimeout(ctx, d.Faults, 3*d.Cfg.RequestTimeout)
		defer cancel()

		resp, err := checkout.PlaceOrder(ctx, &shopv1.PlaceOrderRequest{
			CartId:       in.CartID,
			CustomerId:   in.CustomerID,
			CustomerTier: in.CustomerTier,
			Email:        in.Email,
			ShippingAddress: &shopv1.Address{
				Street:     in.Address.Street,
				City:       in.Address.City,
				Region:     in.Address.Region,
				PostalCode: in.Address.PostalCode,
				Country:    in.Address.Country,
			},
			Card: &shopv1.CardDetails{
				LastFour: in.CardLastFour,
				Brand:    in.CardBrand,
			},
		})
		if err != nil {
			return nil, fmt.Errorf("place order: %w", err)
		}

		return domain.CheckoutResponse{
			OrderID:       resp.GetOrderId(),
			Total:         domain.MoneyFromProto(resp.GetTotal()),
			ShippingCost:  domain.MoneyFromProto(resp.GetShippingCost()),
			TransactionID: resp.GetTransactionId(),
			TrackingID:    resp.GetTrackingId(),
		}, nil
	})

	s.Route("GET /api/orders/{id}", func(ctx context.Context, r *http.Request) (any, error) {
		ctx, cancel := grpcx.WithTimeout(ctx, d.Faults, d.Cfg.RequestTimeout)
		defer cancel()

		resp, err := checkout.GetOrder(ctx, &shopv1.GetOrderRequest{OrderId: r.PathValue("id")})
		if err != nil {
			return nil, fmt.Errorf("get order: %w", err)
		}
		if resp.GetOrderId() == "" {
			return nil, &httpx.NotFoundError{Msg: "order not found"}
		}
		return map[string]any{
			"orderId": resp.GetOrderId(),
			"status":  resp.GetStatus(),
			"total":   domain.MoneyFromProto(resp.GetTotal()),
			"items":   domain.LineItemsFromProto(resp.GetItems()),
		}, nil
	})

	// The browser storefront. Served from this same listener, which keeps it
	// same-origin with the /api routes above — no CORS anywhere. Registered
	// last: the catch-all "/" must not shadow the specific "GET /api/..."
	// patterns, and Go's method-aware mux gives those precedence regardless of
	// registration order.
	if d.Cfg.WebUIEnabled {
		ui, err := newUIHandler()
		if err != nil {
			return fmt.Errorf("storefront ui: %w", err)
		}
		s.Static("/", ui)
		slog.Info("browser storefront enabled")
	}

	d.Admin.SetReady(true)
	return s.Start(ctx)
}

func intParam(r *http.Request, name string, def int32) int32 {
	raw := r.URL.Query().Get(name)
	if raw == "" {
		return def
	}
	v, err := strconv.Atoi(raw)
	if err != nil || v <= 0 {
		return def
	}
	if v > 200 {
		v = 200
	}
	return int32(v)
}
