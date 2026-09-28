// Package partnersim implements partner-sim, deployed three times as
// stripe-sim, carrier-sim and email-sim.
//
// Each one stands in for a public third-party API — PayPal, EasyPost and
// SendGrid in the shipped chart. Callers address the public URL and the
// transport delivers to this in-cluster Service instead (httpx.WithDialTo), so
// the demo needs no internet access and the clean baseline stays error-free.
//
// The stand-in has to be invisible, the way a real provider is, or Causely
// stops modelling the provider as an External service and blames this pod
// instead:
//
//   - No spans. A SERVER span parented to the caller's CLIENT span makes the
//     mediator bridge the public hostname to this in-cluster Service. So this
//     server is plain net/http, deliberately without otelhttp, and the callers
//     strip trace context on the way out.
//   - No logs. The fault store runs Quiet; a real provider's logs are not in
//     your cluster.
//   - Not under Beyla. The chart moves this role's ports out of the range the
//     Causely agent's Beyla instruments.
//
// See docs/causely-setup.md, "External services".
package partnersim

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/causely-oss/tracey-shop/internal/app"
	"github.com/causely-oss/tracey-shop/internal/domain"
	"github.com/causely-oss/tracey-shop/internal/faults"
	"github.com/causely-oss/tracey-shop/internal/transport/httpx"
)

// routes maps every path a caller may use to the acknowledgement code prefix it
// answers with. The provider-shaped paths are what the shipped callers use; the
// short ones are kept so an older caller image still works against a new sim.
var routes = map[string]string{
	"/v2/payments/authorizations": "auth",
	"/v2/shipments":               "trk",
	"/v3/mail/send":               "msg",
	"/charges":                    "auth",
	"/shipments":                  "trk",
	"/messages":                   "msg",
}

// Run starts the partner simulator.
func Run(ctx context.Context, d *app.Deps) error {
	provider := d.Cfg.PartnerName
	if provider == "" {
		provider = d.Cfg.ServiceName
	}
	d.Faults.Quiet()

	mux := http.NewServeMux()
	for path, prefix := range routes {
		mux.Handle("POST "+path, handler(d.Faults, provider, prefix, d.Cfg.PartnerLatency))
	}

	srv := &http.Server{
		Addr:              d.Cfg.HTTPAddr,
		Handler:           mux,
		ReadHeaderTimeout: 10 * time.Second,
	}
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
	}()

	d.Admin.SetReady(true)
	slog.Info("http listener started", slog.String("addr", d.Cfg.HTTPAddr))
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

func handler(store *faults.Store, provider, prefix string, latency time.Duration) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()

		// An injected error answers 503 with a body shaped like a provider's
		// outage response. The caller's CLIENT span records the 503 as
		// http.response.status_code, which is what Causely counts as the
		// provider's error.
		if err := store.Gate(ctx); err != nil {
			writeJSON(w, http.StatusServiceUnavailable, map[string]string{
				"name":    "SERVICE_UNAVAILABLE",
				"message": "The service is temporarily unavailable. Please try again later.",
			})
			return
		}

		var in domain.PartnerRequest
		if err := httpx.DecodeJSON(r, &in); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
			return
		}

		// A third party is never instant; a small fixed delay gives the
		// latency graph in Causely a realistic shape.
		select {
		case <-time.After(latency):
		case <-ctx.Done():
			return
		}

		ref := in.Reference
		if ref == "" {
			ref = domain.NewID("ref")
		}
		writeJSON(w, http.StatusOK, domain.PartnerResponse{
			Provider:  provider,
			Reference: ref,
			Status:    "accepted",
			Code:      prefix + "_" + strings.TrimPrefix(domain.NewID("x"), "x-"),
		})
	})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
