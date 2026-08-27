// Package shipping implements shipping-quote: an HTTP service that calls the
// carrier's HTTP API.
//
// It gives the demo an HTTP -> HTTP branch hanging off checkout, so the topology
// is not uniformly gRPC below the edge.
package shipping

import (
	"context"
	"fmt"
	"net/http"

	"github.com/causely-oss/tracey-shop/internal/app"
	"github.com/causely-oss/tracey-shop/internal/domain"
	"github.com/causely-oss/tracey-shop/internal/transport/httpx"
)

// Rate card, in cents. Deterministic so the demo's totals are reproducible.
const (
	baseRateCents    = 599
	perItemCents     = 149
	perKilogramCents = 220
	expeditedSurchg  = 800
)

// Run starts the shipping HTTP server.
func Run(ctx context.Context, d *app.Deps) error {
	carrier := d.HTTPClient(d.Cfg.CarrierURL)
	s := httpx.NewServer(d.Cfg.ServiceName, d.Cfg.HTTPAddr, d.Faults)

	s.Route("POST /quotes", func(ctx context.Context, r *http.Request) (any, error) {
		var in domain.ShippingQuoteRequest
		if err := httpx.DecodeJSON(r, &in); err != nil {
			return nil, err
		}
		if in.ItemCount <= 0 {
			in.ItemCount = 1
		}

		cost := baseRateCents +
			perItemCents*int64(in.ItemCount) +
			perKilogramCents*int64(in.WeightG)/1000
		if in.Address.Country != "" && in.Address.Country != "US" {
			cost += expeditedSurchg
		}

		// Book the shipment with the carrier to get a tracking number.
		var booking domain.PartnerResponse
		if err := carrier.PostJSON(ctx, "/shipments", domain.PartnerRequest{
			Reference: in.OrderID,
			AmountC:   cost,
			Currency:  "USD",
			Metadata: map[string]string{
				"postalCode": in.Address.PostalCode,
				"country":    in.Address.Country,
			},
		}, &booking); err != nil {
			return nil, fmt.Errorf("carrier booking: %w", err)
		}

		service := "ground"
		eta := "3-5 business days"
		if in.Address.Country != "" && in.Address.Country != "US" {
			service, eta = "international", "7-12 business days"
		}

		return domain.ShippingQuoteResponse{
			Carrier:      booking.Provider,
			Service:      service,
			Cost:         domain.USD(cost),
			EstimatedETA: eta,
			TrackingID:   booking.Code,
		}, nil
	})

	d.Admin.SetReady(true)
	return s.Start(ctx)
}
