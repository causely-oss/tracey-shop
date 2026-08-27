// Package partnersim implements partner-sim, deployed three times as
// stripe-sim, carrier-sim and email-sim.
//
// Standing in for third parties with a real in-cluster Service — rather than
// calling an unreachable external hostname — keeps the demo's leaf dependencies
// resolvable in Causely's topology and keeps the clean baseline error-free with
// no internet access.
package partnersim

import (
	"context"
	"net/http"
	"strings"
	"time"

	"github.com/causely-oss/tracey-shop/internal/app"
	"github.com/causely-oss/tracey-shop/internal/domain"
	"github.com/causely-oss/tracey-shop/internal/transport/httpx"
)

// Run starts the partner simulator.
func Run(ctx context.Context, d *app.Deps) error {
	provider := d.Cfg.PartnerName
	if provider == "" {
		provider = d.Cfg.ServiceName
	}
	latency := d.Cfg.PartnerLatency

	s := httpx.NewServer(d.Cfg.ServiceName, d.Cfg.HTTPAddr, d.Faults)

	// One handler serves all three roles' endpoints, so a single deployment
	// template covers every partner.
	for _, path := range []string{"/charges", "/shipments", "/messages"} {
		prefix := codePrefix(path)
		s.Route("POST "+path, func(ctx context.Context, r *http.Request) (any, error) {
			var in domain.PartnerRequest
			if err := httpx.DecodeJSON(r, &in); err != nil {
				return nil, err
			}

			// A third party is never instant; a small fixed delay gives the
			// latency graph in Causely a realistic shape.
			select {
			case <-time.After(latency):
			case <-ctx.Done():
				return nil, ctx.Err()
			}

			ref := in.Reference
			if ref == "" {
				ref = domain.NewID("ref")
			}
			return domain.PartnerResponse{
				Provider:  provider,
				Reference: ref,
				Status:    "accepted",
				Code:      prefix + "_" + strings.TrimPrefix(domain.NewID("x"), "x-"),
			}, nil
		})
	}

	d.Admin.SetReady(true)
	return s.Start(ctx)
}

// codePrefix gives each partner's acknowledgement a recognisable code shape.
func codePrefix(path string) string {
	switch path {
	case "/charges":
		return "auth"
	case "/shipments":
		return "trk"
	default:
		return "msg"
	}
}
