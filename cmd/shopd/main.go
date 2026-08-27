// Command shopd is the single binary behind every service in the demo.
//
// The ROLE environment variable selects which service the process runs. One
// image and one build keeps the demo maintainable, while Helm still renders each
// role as its own Deployment and Service so Causely sees a genuine multi-service
// topology with distinct service.name values.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"sort"
	"strings"
	"syscall"
	"time"

	"github.com/causely-oss/tracey-shop/internal/admin"
	"github.com/causely-oss/tracey-shop/internal/app"
	"github.com/causely-oss/tracey-shop/internal/config"
	"github.com/causely-oss/tracey-shop/internal/faults"
	"github.com/causely-oss/tracey-shop/internal/obs"
	"github.com/causely-oss/tracey-shop/internal/services/cart"
	"github.com/causely-oss/tracey-shop/internal/services/catalog"
	"github.com/causely-oss/tracey-shop/internal/services/checkout"
	"github.com/causely-oss/tracey-shop/internal/services/fraud"
	"github.com/causely-oss/tracey-shop/internal/services/inventory"
	"github.com/causely-oss/tracey-shop/internal/services/ledger"
	"github.com/causely-oss/tracey-shop/internal/services/loadgen"
	"github.com/causely-oss/tracey-shop/internal/services/notification"
	"github.com/causely-oss/tracey-shop/internal/services/partnersim"
	"github.com/causely-oss/tracey-shop/internal/services/payment"
	"github.com/causely-oss/tracey-shop/internal/services/pricing"
	"github.com/causely-oss/tracey-shop/internal/services/risk"
	"github.com/causely-oss/tracey-shop/internal/services/shipping"
	"github.com/causely-oss/tracey-shop/internal/services/storefront"
)

// roles maps a ROLE value to its entry point.
var roles = map[string]app.RunFunc{
	"storefront-bff":      storefront.Run,
	"catalog-api":         catalog.Run,
	"cart-service":        cart.Run,
	"checkout-api":        checkout.Run,
	"inventory-svc":       inventory.Run,
	"pricing-engine":      pricing.Run,
	"payment-gw":          payment.Run,
	"shipping-quote":      shipping.Run,
	"ledger-svc":          ledger.Run,
	"fraud-detector":      fraud.Run,
	"risk-model":          risk.Run,
	"notification-worker": notification.Run,
	"partner-sim":         partnersim.Run,
	"loadgen":             loadgen.Run,
}

func main() {
	if err := run(); err != nil {
		slog.Error("fatal", slog.Any("err", err))
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.Load()
	if err != nil {
		return err
	}
	obs.SetupLogging(cfg)

	runFn, ok := roles[cfg.Role]
	if !ok {
		return fmt.Errorf("unknown ROLE %q; valid roles: %s", cfg.Role, strings.Join(roleNames(), ", "))
	}

	// Signal-driven shutdown so Kubernetes rollouts are graceful and do not
	// register as errors in the baseline.
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	shutdownTracing, err := obs.Setup(ctx, cfg)
	if err != nil {
		return fmt.Errorf("setup telemetry: %w", err)
	}
	defer func() {
		flushCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := shutdownTracing(flushCtx); err != nil {
			slog.Warn("tracing shutdown", slog.Any("err", err))
		}
	}()

	store := faults.NewStore(cfg.ServiceName)
	adminSrv := admin.New(cfg.AdminAddr, store)
	deps := app.New(cfg, store, adminSrv)
	defer deps.Close()

	// The admin listener comes up first so liveness probes succeed while the
	// role is still connecting to its backends.
	adminErr := make(chan error, 1)
	go func() { adminErr <- adminSrv.Start(ctx) }()

	// role and service are already bound to the default logger.
	slog.Info("starting",
		slog.String("version", cfg.Version),
		slog.String("otlp_endpoint", cfg.OTLPEndpoint))

	runErr := make(chan error, 1)
	go func() { runErr <- runFn(ctx, deps) }()

	select {
	case err := <-runErr:
		if err != nil && !errors.Is(err, context.Canceled) {
			return fmt.Errorf("role %s: %w", cfg.Role, err)
		}
	case err := <-adminErr:
		if err != nil {
			return fmt.Errorf("admin server: %w", err)
		}
	case <-ctx.Done():
		slog.Info("shutdown signal received")
		// Give the role a moment to drain before the deferred cleanups run.
		select {
		case <-runErr:
		case <-time.After(15 * time.Second):
			slog.Warn("role did not exit within the drain window")
		}
	}
	return nil
}

func roleNames() []string {
	names := make([]string, 0, len(roles))
	for name := range roles {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}
