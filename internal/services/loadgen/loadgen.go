// Package loadgen implements the traffic generator.
//
// It drives only storefront-bff over HTTP, so every request exercises a real
// path through the graph. The rate is adjustable at runtime via the admin
// endpoint (see scripts/load.sh), which matters for a demo: you can raise load
// to make a symptom fire faster without a helm upgrade or a pod restart.
//
// Its own outbound spans are CLIENT spans against storefront-bff, so loadgen
// appears in Causely's topology as the traffic source.
package loadgen

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"math/rand"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/causely-oss/tracey-shop/internal/admin"
	"github.com/causely-oss/tracey-shop/internal/app"
	"github.com/causely-oss/tracey-shop/internal/domain"
	"github.com/causely-oss/tracey-shop/internal/transport/httpx"
)

// seededProducts must match the row count seeded by internal/store.
const seededProducts = 200

var (
	categories = []string{"electronics", "apparel", "home", "outdoors", "books", "toys", "grocery", "beauty"}
	searches   = []string{"Compact", "Premium", "Everyday", "Rugged", "Classic", "Modern", "Deluxe", "Essential"}
	tiers      = []string{"standard", "standard", "standard", "gold", "gold", "platinum"}
	countries  = []string{"US", "US", "US", "US", "CA", "GB", "DE"}
)

// generator holds the mutable rate configuration.
type generator struct {
	client *httpx.Client
	mix    []weightedAction

	rateMu      sync.RWMutex
	rps         float64
	concurrency int

	completed atomic.Int64
	failed    atomic.Int64
}

type weightedAction struct {
	name   string
	weight int
	run    func(ctx context.Context, g *generator, rng *rand.Rand) error
}

// Run starts the load generator.
func Run(ctx context.Context, d *app.Deps) error {
	g := &generator{
		client:      d.HTTPClient(d.Cfg.LoadTargetURL),
		rps:         d.Cfg.LoadRPS,
		concurrency: d.Cfg.LoadConcurrency,
	}
	g.mix = buildMix(d.Cfg.LoadMix)
	if len(g.mix) == 0 {
		return fmt.Errorf("load mix is empty: every weight is zero")
	}

	registerAdminRoutes(d, g)
	d.Admin.SetReady(true)

	// Give the rest of the deployment a moment to become ready, so the first
	// requests of a fresh install do not show up as errors in the baseline.
	select {
	case <-time.After(15 * time.Second):
	case <-ctx.Done():
		return nil
	}

	slog.Info("load generator starting",
		slog.String("target", d.Cfg.LoadTargetURL),
		slog.Float64("rps", g.rps),
		slog.Int("concurrency", g.concurrency))

	return g.run(ctx)
}

// run paces work with a ticker whose interval tracks the current rate, and
// executes each action on a bounded worker pool.
func (g *generator) run(ctx context.Context) error {
	jobs := make(chan struct{}, 1024)

	var wg sync.WaitGroup
	for i := 0; i < g.currentConcurrency(); i++ {
		wg.Add(1)
		go func(seed int64) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(seed))
			for range jobs {
				if ctx.Err() != nil {
					return
				}
				g.execute(ctx, rng)
			}
		}(time.Now().UnixNano() + int64(i)*7919)
	}

	go g.reportLoop(ctx)

	ticker := time.NewTicker(g.tickInterval())
	defer ticker.Stop()
	current := g.currentRPS()

	for {
		select {
		case <-ctx.Done():
			close(jobs)
			wg.Wait()
			return nil
		case <-ticker.C:
			// Re-arm the ticker when the rate was changed at runtime.
			if rps := g.currentRPS(); rps != current {
				current = rps
				ticker.Reset(g.tickInterval())
			}
			select {
			case jobs <- struct{}{}:
			default:
				// Workers are saturated: drop the tick rather than growing an
				// unbounded backlog, and let the report loop show the shortfall.
			}
		}
	}
}

func (g *generator) execute(ctx context.Context, rng *rand.Rand) {
	action := g.pick(rng)

	// Each action gets its own bounded context so one slow request cannot
	// occupy a worker indefinitely.
	actionCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	if err := action.run(actionCtx, g, rng); err != nil {
		g.failed.Add(1)
		slog.Debug("action failed",
			slog.String("action", action.name),
			slog.Any("err", err))
		return
	}
	g.completed.Add(1)
}

func (g *generator) pick(rng *rand.Rand) weightedAction {
	total := 0
	for _, a := range g.mix {
		total += a.weight
	}
	n := rng.Intn(total)
	for _, a := range g.mix {
		if n < a.weight {
			return a
		}
		n -= a.weight
	}
	return g.mix[len(g.mix)-1]
}

func (g *generator) reportLoop(ctx context.Context) {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			slog.Info("load summary",
				slog.Int64("completed", g.completed.Load()),
				slog.Int64("failed", g.failed.Load()),
				slog.Float64("rps", g.currentRPS()))
		}
	}
}

func (g *generator) currentRPS() float64 {
	g.rateMu.RLock()
	defer g.rateMu.RUnlock()
	return g.rps
}

func (g *generator) currentConcurrency() int {
	g.rateMu.RLock()
	defer g.rateMu.RUnlock()
	if g.concurrency < 1 {
		return 1
	}
	return g.concurrency
}

func (g *generator) tickInterval() time.Duration {
	rps := g.currentRPS()
	if rps <= 0 {
		// Paused: tick slowly and drop the work.
		return time.Second
	}
	return time.Duration(float64(time.Second) / rps)
}

// ---------------------------------------------------------------------------
// Actions
// ---------------------------------------------------------------------------

func buildMix(weights map[string]int) []weightedAction {
	all := []weightedAction{
		{name: "browse", weight: weights["browse"], run: actionBrowse},
		{name: "search", weight: weights["search"], run: actionSearch},
		{name: "viewProduct", weight: weights["viewProduct"], run: actionViewProduct},
		{name: "addToCart", weight: weights["addToCart"], run: actionAddToCart},
		{name: "checkout", weight: weights["checkout"], run: actionCheckout},
	}
	out := make([]weightedAction, 0, len(all))
	for _, a := range all {
		if a.weight > 0 {
			out = append(out, a)
		}
	}
	return out
}

func actionBrowse(ctx context.Context, g *generator, rng *rand.Rand) error {
	category := categories[rng.Intn(len(categories))]
	var out struct {
		Products []domain.Product `json:"products"`
	}
	return g.client.GetJSON(ctx,
		fmt.Sprintf("/api/products?category=%s&limit=20", category), &out)
}

func actionSearch(ctx context.Context, g *generator, rng *rand.Rand) error {
	var out struct {
		Products []domain.Product `json:"products"`
	}
	return g.client.GetJSON(ctx,
		"/api/search?q="+searches[rng.Intn(len(searches))], &out)
}

func actionViewProduct(ctx context.Context, g *generator, rng *rand.Rand) error {
	var out struct {
		Product domain.Product `json:"product"`
	}
	return g.client.GetJSON(ctx, "/api/products/"+randomProductID(rng), &out)
}

func actionAddToCart(ctx context.Context, g *generator, rng *rand.Rand) error {
	cartID := randomCartID(rng)
	var cart domain.Cart
	return g.client.PostJSON(ctx, "/api/cart/"+cartID+"/items", domain.AddToCartRequest{
		ProductID: randomProductID(rng),
		Quantity:  int32(1 + rng.Intn(3)),
	}, &cart)
}

// actionCheckout is the full journey: create a cart, add two items, then check
// out. Doing all three in one action guarantees checkout never sees an empty
// cart, which is what keeps the clean baseline free of 4xx responses.
func actionCheckout(ctx context.Context, g *generator, rng *rand.Rand) error {
	cartID := domain.NewID("cart")

	for i := 0; i < 2; i++ {
		var cart domain.Cart
		if err := g.client.PostJSON(ctx, "/api/cart/"+cartID+"/items", domain.AddToCartRequest{
			ProductID: randomProductID(rng),
			Quantity:  int32(1 + rng.Intn(2)),
		}, &cart); err != nil {
			return fmt.Errorf("seed cart: %w", err)
		}
	}

	country := countries[rng.Intn(len(countries))]
	var out domain.CheckoutResponse
	return g.client.PostJSON(ctx, "/api/checkout", domain.CheckoutRequest{
		CartID:       cartID,
		CustomerID:   fmt.Sprintf("cust-%04d", rng.Intn(500)),
		CustomerTier: tiers[rng.Intn(len(tiers))],
		Email:        fmt.Sprintf("shopper%04d@example.com", rng.Intn(500)),
		Address: domain.Address{
			Street:     fmt.Sprintf("%d Market St", 100+rng.Intn(900)),
			City:       "Springfield",
			Region:     "CA",
			PostalCode: fmt.Sprintf("9%04d", rng.Intn(10000)),
			Country:    country,
		},
		CardLastFour: fmt.Sprintf("%04d", rng.Intn(10000)),
		CardBrand:    "visa",
	}, &out)
}

func randomProductID(rng *rand.Rand) string {
	return fmt.Sprintf("P%04d", 1+rng.Intn(seededProducts))
}

// randomCartID reuses a bounded pool of ids so carts accumulate items the way
// real sessions do.
func randomCartID(rng *rand.Rand) string {
	return fmt.Sprintf("cart-session-%03d", rng.Intn(200))
}

// ---------------------------------------------------------------------------
// Admin control
// ---------------------------------------------------------------------------

type loadPatch struct {
	RPS         *float64 `json:"rps"`
	Concurrency *int     `json:"concurrency"`
}

func registerAdminRoutes(d *app.Deps, g *generator) {
	d.Admin.Handle("/admin/load", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			admin.WriteJSON(w, http.StatusOK, g.status())

		case http.MethodPost, http.MethodPut, http.MethodPatch:
			body, err := io.ReadAll(io.LimitReader(r.Body, 1<<16))
			if err != nil {
				admin.WriteJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
				return
			}
			var patch loadPatch
			if len(body) > 0 {
				if err := json.Unmarshal(body, &patch); err != nil {
					admin.WriteJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
					return
				}
			}

			g.rateMu.Lock()
			if patch.RPS != nil && *patch.RPS >= 0 {
				g.rps = *patch.RPS
			}
			// Concurrency is recorded but only takes effect on restart: the
			// worker pool is sized once, and resizing it mid-flight would risk
			// dropping in-flight requests and dirtying the baseline.
			if patch.Concurrency != nil && *patch.Concurrency > 0 {
				g.concurrency = *patch.Concurrency
			}
			g.rateMu.Unlock()

			// Deliberately Debug, not Warn — same reason as the fault store's
			// messages in internal/faults.
			//
			// Causely promotes WARN/ERROR container logs to root-cause evidence.
			// At Warn, this single line ("load configuration changed", count: 1)
			// was picked up as the evidence for a *Critical* "Service Malfunction"
			// root cause on checkout-api, whose description then blamed "load
			// configuration changes and deployment settings" — while the real
			// cause, a 35% payment-gw failure rate, sat below it as a separate
			// High root cause. Changing the traffic rate is a control-plane
			// action, not an application fault, and must not read as one.
			slog.Debug("load configuration changed", slog.Any("status", g.status()))
			admin.WriteJSON(w, http.StatusOK, g.status())

		default:
			w.Header().Set("Allow", "GET, POST, PUT, PATCH")
			admin.WriteJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "method not allowed"})
		}
	})
}

func (g *generator) status() map[string]any {
	g.rateMu.RLock()
	rps, concurrency := g.rps, g.concurrency
	g.rateMu.RUnlock()
	return map[string]any{
		"rps":                  rps,
		"concurrency":          concurrency,
		"concurrencyAppliesOn": "restart",
		"completed":            g.completed.Load(),
		"failed":               g.failed.Load(),
	}
}
