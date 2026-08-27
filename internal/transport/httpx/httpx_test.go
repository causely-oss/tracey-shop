package httpx

import "testing"

// TestHostPortFromURL matters more than it looks: the values it returns become
// the server.address and server.port attributes on every outbound HTTP CLIENT
// span, and those are what Causely uses to resolve the dependency edge. A wrong
// host here means a missing edge in the topology.
func TestHostPortFromURL(t *testing.T) {
	cases := []struct {
		in       string
		wantHost string
		wantPort int
	}{
		{"http://cart-service:8081", "cart-service", 8081},
		{"http://tracey-shop-cart-service:8081", "tracey-shop-cart-service", 8081},
		{"http://cart-service:8081/carts/abc", "cart-service", 8081},
		{"http://cart-service", "cart-service", 80},
		{"https://partner.example.com", "partner.example.com", 443},
		{"https://partner.example.com:8443/charges", "partner.example.com", 8443},
		{"cart-service:8081", "cart-service", 8081},
		{"http://cart-service.other-ns.svc.cluster.local:8081", "cart-service.other-ns.svc.cluster.local", 8081},
	}

	for _, tc := range cases {
		host, port := hostPortFromURL(tc.in)
		if host != tc.wantHost || port != tc.wantPort {
			t.Errorf("hostPortFromURL(%q) = (%q, %d), want (%q, %d)",
				tc.in, host, port, tc.wantHost, tc.wantPort)
		}
	}
}

func TestRoutePath(t *testing.T) {
	cases := map[string]string{
		"GET /api/products":      "/api/products",
		"POST /carts/{id}/items": "/carts/{id}/items",
		"/api/products":          "/api/products",
		"DELETE /carts/{id}":     "/carts/{id}",
	}
	for in, want := range cases {
		if got := routePath(in); got != want {
			t.Errorf("routePath(%q) = %q, want %q", in, got, want)
		}
	}
}
