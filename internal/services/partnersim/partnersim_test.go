package partnersim

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/causely-oss/tracey-shop/internal/faults"
)

// TestInjectedErrorAnswers503 pins the provider-outage contract. Causely counts
// a third party's error from http.response.status_code on the caller's CLIENT
// span, and only >=500 counts — so the stand-in must answer a server error, and
// 503 is what a provider in an outage actually returns.
func TestInjectedErrorAnswers503(t *testing.T) {
	store := faults.NewStore("stripe-sim")
	store.Quiet()
	rate := 1.0
	store.Apply(faults.Patch{ErrorRate: &rate})

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/v2/payments/authorizations", strings.NewReader(`{"reference":"txn-1"}`))
	handler(store, "paypal", "auth", 0).ServeHTTP(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", rec.Code)
	}
	if body := rec.Body.String(); strings.Contains(strings.ToLower(body), "inject") || strings.Contains(strings.ToLower(body), "fault") {
		t.Errorf("outage body reveals the injection: %s", body)
	}
}

func TestHealthyRequestIsAccepted(t *testing.T) {
	store := faults.NewStore("email-sim")

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/v3/mail/send", strings.NewReader(`{"reference":"ord-1","to":"a@example.com"}`))
	handler(store, "sendgrid", "msg", 0).ServeHTTP(rec, req)

	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `"code":"msg_`) {
		t.Fatalf("status = %d body = %s, want 200 with a msg_ code", rec.Code, rec.Body.String())
	}
}
