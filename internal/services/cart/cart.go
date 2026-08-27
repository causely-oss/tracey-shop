// Package cart implements cart-service: an HTTP service backed entirely by
// Valkey.
//
// It is the only HTTP hop in the middle of the synchronous graph, which is what
// gives the demo an HTTP -> HTTP -> gRPC protocol transition for Causely to
// model.
package cart

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/causely-oss/tracey-shop/internal/app"
	"github.com/causely-oss/tracey-shop/internal/domain"
	"github.com/causely-oss/tracey-shop/internal/transport/httpx"
	"github.com/causely-oss/tracey-shop/internal/transport/redisx"
)

// cartTTL keeps abandoned carts from accumulating in Valkey forever.
const cartTTL = 2 * time.Hour

// Run starts the cart HTTP server.
func Run(ctx context.Context, d *app.Deps) error {
	cache, err := d.Redis(ctx)
	if err != nil {
		return fmt.Errorf("redis: %w", err)
	}

	s := httpx.NewServer(d.Cfg.ServiceName, d.Cfg.HTTPAddr, d.Faults)

	s.Route("GET /carts/{id}", func(ctx context.Context, r *http.Request) (any, error) {
		id := r.PathValue("id")
		if id == "" {
			return nil, &httpx.BadRequestError{Msg: "cart id is required"}
		}
		return loadCart(ctx, cache, id)
	})

	s.Route("POST /carts/{id}/items", func(ctx context.Context, r *http.Request) (any, error) {
		id := r.PathValue("id")
		if id == "" {
			return nil, &httpx.BadRequestError{Msg: "cart id is required"}
		}

		var in domain.AddToCartRequest
		if err := httpx.DecodeJSON(r, &in); err != nil {
			return nil, err
		}
		if in.ProductID == "" {
			return nil, &httpx.BadRequestError{Msg: "productId is required"}
		}
		if in.Quantity <= 0 {
			in.Quantity = 1
		}

		cart, err := loadCart(ctx, cache, id)
		if err != nil {
			return nil, err
		}

		merged := false
		for i := range cart.Items {
			if cart.Items[i].ProductID == in.ProductID {
				cart.Items[i].Quantity += in.Quantity
				merged = true
				break
			}
		}
		if !merged {
			cart.Items = append(cart.Items, domain.CartItem{
				ProductID: in.ProductID,
				Quantity:  in.Quantity,
			})
		}
		cart.UpdatedAt = time.Now().UTC()

		if err := cache.SetJSON(ctx, cartKey(id), cart, cartTTL); err != nil {
			return nil, fmt.Errorf("persist cart: %w", err)
		}
		return cart, nil
	})

	// checkout-api calls this after a successful order so the next checkout for
	// the same cart id starts from empty.
	s.Route("POST /carts/{id}/clear", func(ctx context.Context, r *http.Request) (any, error) {
		id := r.PathValue("id")
		if id == "" {
			return nil, &httpx.BadRequestError{Msg: "cart id is required"}
		}
		if err := cache.Del(ctx, cartKey(id)); err != nil {
			return nil, fmt.Errorf("clear cart: %w", err)
		}
		return map[string]string{"status": "cleared", "cartId": id}, nil
	})

	s.Route("DELETE /carts/{id}", func(ctx context.Context, r *http.Request) (any, error) {
		id := r.PathValue("id")
		if id == "" {
			return nil, &httpx.BadRequestError{Msg: "cart id is required"}
		}
		if err := cache.Del(ctx, cartKey(id)); err != nil {
			return nil, fmt.Errorf("delete cart: %w", err)
		}
		return map[string]string{"status": "deleted", "cartId": id}, nil
	})

	d.Admin.SetReady(true)
	return s.Start(ctx)
}

// loadCart returns the stored cart, or an empty one. A missing cart is not an
// error: the demo's happy path checks out carts that were only just created.
func loadCart(ctx context.Context, cache *redisx.Client, id string) (domain.Cart, error) {
	var cart domain.Cart
	found, err := cache.GetJSON(ctx, cartKey(id), &cart)
	if err != nil {
		return domain.Cart{}, fmt.Errorf("load cart %s: %w", id, err)
	}
	if !found || cart.ID == "" {
		cart = domain.Cart{ID: id, Items: []domain.CartItem{}, UpdatedAt: time.Now().UTC()}
	}
	return cart, nil
}

func cartKey(id string) string { return "cart:" + id }
