package storefront

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/causely-oss/tracey-shop/internal/app"
	"github.com/causely-oss/tracey-shop/internal/domain"
	"github.com/causely-oss/tracey-shop/internal/faults"
	"github.com/causely-oss/tracey-shop/internal/transport/httpx"
)

// TestPayLaterOfferDegradesGracefully pins the product-page contract. The offer
// comes from the payment provider, and when the provider fails the page must
// still render, just without the offer. A product page that failed along with
// the provider would add a storefront error of our own to the incident, which
// is exactly what the provider-outage scenario is meant to show Causely
// exonerating.
func TestPayLaterOfferDegradesGracefully(t *testing.T) {
	status := http.StatusOK
	provider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(status)
		_, _ = w.Write([]byte(`{"provider":"paypal","status":"accepted","code":"fin_1"}`))
	}))
	defer provider.Close()

	d := &app.Deps{Faults: faults.NewStore("storefront-bff")}
	client := httpx.NewClient("https://api.paypal.com", 5*time.Second, d.Faults, httpx.WithDialTo(provider.URL))
	product := domain.Product{ID: "P0001", Price: domain.Money{Cents: 1999, Currency: "USD"}}

	offer := payLaterOffer(context.Background(), d, client, product)
	if offer == nil || offer.Installments != 4 || offer.Installment.Cents != 500 {
		t.Fatalf("healthy provider: offer = %+v, want 4 x 500", offer)
	}

	status = http.StatusServiceUnavailable
	if offer := payLaterOffer(context.Background(), d, client, product); offer != nil {
		t.Fatalf("failing provider: offer = %+v, want nil so the page renders without it", offer)
	}
}
