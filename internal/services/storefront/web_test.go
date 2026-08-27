package storefront

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/causely-oss/tracey-shop/internal/domain"
)

func handler(t *testing.T) http.Handler {
	t.Helper()
	h, err := newUIHandler()
	if err != nil {
		t.Fatalf("newUIHandler: %v", err)
	}
	return h
}

func get(t *testing.T, h http.Handler, path string) *httptest.ResponseRecorder {
	t.Helper()
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
	return rec
}

func TestShellIsHTML(t *testing.T) {
	h := handler(t)
	rec := get(t, h, "/")
	if ct := rec.Header().Get("Content-Type"); !strings.HasPrefix(ct, "text/html") {
		t.Errorf("Content-Type = %q, want text/html", ct)
	}
	body := rec.Body.String()
	// Stable ids on purpose: any analytics or RUM tool pointed at this page
	// keys its event and funnel definitions off these selectors, and renaming
	// one silently breaks those definitions outside this repo.
	for _, id := range []string{
		`id="search"`, `id="cart-count"`, `id="error-banner"`, `id="view"`,
	} {
		if !strings.Contains(body, id) {
			t.Errorf("shell is missing the stable selector %s", id)
		}
	}
}

func TestAssetsAreServed(t *testing.T) {
	h := handler(t)

	for path, want := range map[string]string{
		"/app.js":    "placeOrder",
		"/style.css": ".product-card",
	} {
		rec := get(t, h, path)
		if rec.Code != http.StatusOK {
			t.Errorf("GET %s = %d, want 200", path, rec.Code)
			continue
		}
		if !strings.Contains(rec.Body.String(), want) {
			t.Errorf("GET %s does not contain %q", path, want)
		}
	}
}

// TestSPADeepLinksReturnTheShell — the client router owns these paths, so a
// refresh or a pasted link has to return the HTML rather than a 404.
func TestSPADeepLinksReturnTheShell(t *testing.T) {
	h := handler(t)
	for _, path := range []string{"/product/P0007", "/cart", "/checkout", "/order/ord-abc123", "/search"} {
		rec := get(t, h, path)
		if rec.Code != http.StatusOK {
			t.Errorf("GET %s = %d, want 200 (the SPA shell)", path, rec.Code)
		}
		if !strings.Contains(rec.Body.String(), `id="view"`) {
			t.Errorf("GET %s did not return the shell", path)
		}
	}
}

// TestUnmatchedAPIPathsAre404 — returning the HTML shell for a bad /api/ call
// would turn a wrong request into a confusing 200.
func TestUnmatchedAPIPathsAre404(t *testing.T) {
	h := handler(t)
	rec := get(t, h, "/api/does-not-exist")
	if rec.Code != http.StatusNotFound {
		t.Errorf("GET /api/does-not-exist = %d, want 404", rec.Code)
	}
	if strings.Contains(rec.Body.String(), "<html") {
		t.Error("served the HTML shell for an /api/ path")
	}
}

// TestUIRequestBodiesSatisfyDisallowUnknownFields is the guard that matters most
// for keeping the Causely baseline clean.
//
// storefront-bff decodes POST bodies with DisallowUnknownFields, so a single
// stray or renamed key turns every browser checkout into a 400 — which shows up
// as demo traffic errors that have nothing to do with the scenario being run.
// These fixtures mirror exactly what app.js sends.
func TestUIRequestBodiesSatisfyDisallowUnknownFields(t *testing.T) {
	strict := func(t *testing.T, raw string, into any) {
		t.Helper()
		dec := json.NewDecoder(strings.NewReader(raw))
		dec.DisallowUnknownFields()
		if err := dec.Decode(into); err != nil {
			t.Errorf("the UI's request body no longer decodes into the backend struct: %v\nbody: %s", err, raw)
		}
	}

	var add domain.AddToCartRequest
	strict(t, `{"productId":"P0007","quantity":2}`, &add)

	var checkout domain.CheckoutRequest
	strict(t, `{
      "cartId":"cart-deadbeefdeadbeef",
      "customerId":"web-adbeef",
      "customerTier":"gold",
      "email":"shopper0001@example.com",
      "address":{"street":"123 Market St","city":"Springfield","region":"CA",
                 "postalCode":"94105","country":"US"},
      "cardLastFour":"4242",
      "cardBrand":"visa"
    }`, &checkout)

	// And the same field names must actually appear in the shipped JS, so a
	// backend rename cannot pass this test while leaving the UI broken.
	js, err := webFS.ReadFile("web/app.js")
	if err != nil {
		t.Fatalf("read app.js: %v", err)
	}
	for _, key := range []string{
		"productId", "quantity", "cartId", "customerId", "customerTier",
		"email", "address", "street", "city", "region", "postalCode",
		"country", "cardLastFour", "cardBrand",
	} {
		if !strings.Contains(string(js), key) {
			t.Errorf("app.js never mentions %q — the UI and the backend struct have diverged", key)
		}
	}
}

// TestAssetsDoNotReferenceExternalResources is what lets the demo run on a
// cluster with no egress: the page must make no external requests at all.
func TestAssetsDoNotReferenceExternalResources(t *testing.T) {
	for _, name := range []string{"web/app.js", "web/style.css"} {
		b, err := webFS.ReadFile(name)
		if err != nil {
			t.Fatalf("read %s: %v", name, err)
		}
		body := string(b)
		for _, needle := range []string{"http://", "https://", "//cdn.", "fonts.googleapis"} {
			if strings.Contains(body, needle) {
				t.Errorf("%s references an external resource (%q); assets must be self-contained",
					name, needle)
			}
		}
	}
}

func TestShellIsNotCached(t *testing.T) {
	h := handler(t)
	if cc := get(t, h, "/").Header().Get("Cache-Control"); cc != "no-store" {
		t.Errorf("shell Cache-Control = %q, want no-store (it is templated per deploy)", cc)
	}
}
