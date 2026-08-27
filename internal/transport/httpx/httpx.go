// Package httpx provides the demo's instrumented HTTP server and client.
//
// otelhttp v0.62 emits the stable HTTP semantic conventions by default, so
// client spans already carry server.address, server.port, url.full and
// http.response.status_code — exactly the attributes Causely reads to resolve
// an HTTP dependency edge. The wrapper below re-asserts server.address and
// server.port anyway, because that single attribute is what decides whether the
// edge appears in the topology at all.
package httpx

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strconv"
	"time"

	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	semconv "go.opentelemetry.io/otel/semconv/v1.34.0"
	"go.opentelemetry.io/otel/trace"

	"github.com/causely-oss/tracey-shop/internal/faults"
)

// ---------------------------------------------------------------------------
// Server
// ---------------------------------------------------------------------------

// Server is an instrumented HTTP server for one role.
type Server struct {
	addr   string
	mux    *http.ServeMux
	srv    *http.Server
	faults *faults.Store
	name   string
}

// NewServer builds a server whose handlers are wrapped in otelhttp, producing
// SERVER spans with the route as the span name.
func NewServer(name, addr string, store *faults.Store) *Server {
	return &Server{
		addr:   addr,
		mux:    http.NewServeMux(),
		faults: store,
		name:   name,
	}
}

// Handler is a business handler that returns a value to encode as JSON.
type Handler func(ctx context.Context, r *http.Request) (any, error)

// Route registers a JSON handler at pattern. The handler runs behind the fault
// gate, so latency/error/leak injection applies uniformly across every service.
//
// pattern uses net/http's method-aware syntax, e.g. "GET /api/products".
func (s *Server) Route(pattern string, h Handler) {
	route := pattern
	s.mux.Handle(pattern, otelhttp.NewHandler(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			ctx := r.Context()

			if err := s.faults.Gate(ctx); err != nil {
				status := http.StatusInternalServerError
				if ctx.Err() != nil {
					status = http.StatusGatewayTimeout
				}
				recordServerError(ctx, err)
				writeJSON(w, status, map[string]string{"error": err.Error()})
				return
			}

			out, err := h(ctx, r)
			if err != nil {
				recordServerError(ctx, err)
				writeJSON(w, statusFor(err), map[string]string{"error": err.Error()})
				return
			}
			if out == nil {
				w.WriteHeader(http.StatusNoContent)
				return
			}
			writeJSON(w, http.StatusOK, out)
		}),
		route,
		otelhttp.WithSpanNameFormatter(func(operation string, r *http.Request) string {
			return r.Method + " " + routePath(operation)
		}),
	))
}

// Static registers a raw http.Handler, for serving the embedded browser UI.
//
// It deliberately bypasses the two things Route applies:
//
//   - The fault gate. Asset GETs are not application requests; giving them
//     injected latency or errors would make the page itself fail during a
//     scenario, which is not the failure being demonstrated.
//   - otelhttp. Tracing "/" and "/app.js" would create HTTPPath entities in
//     Causely and shift storefront-bff's latency and error-rate baseline, which
//     every scenario depends on staying clean. Same reasoning as the untraced
//     admin port in internal/admin.
//
// The browser's /api/* calls still go through Route, so they are traced and
// fault-gated exactly like the load generator's.
//
// Go's method-aware mux gives the specific "GET /api/..." patterns precedence
// over a catch-all "/" registered here.
func (s *Server) Static(pattern string, h http.Handler) {
	s.mux.Handle(pattern, h)
}

// Start serves until the context is cancelled.
func (s *Server) Start(ctx context.Context) error {
	s.srv = &http.Server{
		Addr:              s.addr,
		Handler:           s.mux,
		ReadHeaderTimeout: 10 * time.Second,
	}
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = s.srv.Shutdown(shutdownCtx)
	}()

	slog.Info("http listener started", slog.String("addr", s.addr))
	if err := s.srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		return err
	}
	return nil
}

// ---------------------------------------------------------------------------
// Client
// ---------------------------------------------------------------------------

// Client is an instrumented HTTP client pointed at one downstream base URL.
type Client struct {
	base    string
	host    string
	port    int
	hc      *http.Client
	faults  *faults.Store
	timeout time.Duration
}

// NewClient builds a client for baseURL. The peer attributes are derived once
// from the URL and pinned onto every CLIENT span.
func NewClient(baseURL string, timeout time.Duration, store *faults.Store) *Client {
	host, port := hostPortFromURL(baseURL)

	base := &http.Transport{
		MaxIdleConns:        100,
		MaxIdleConnsPerHost: 32,
		IdleConnTimeout:     90 * time.Second,
	}

	// Order matters: otelhttp.NewTransport creates the CLIENT span and then
	// calls the base RoundTripper, so peerPinner sees a context that already
	// carries the span and can assert the peer attributes on it.
	rt := otelhttp.NewTransport(&peerPinner{
		next: base,
		attrs: []attribute.KeyValue{
			semconv.ServerAddress(host),
			semconv.ServerPort(port),
		},
	})

	return &Client{
		base:    baseURL,
		host:    host,
		port:    port,
		hc:      &http.Client{Transport: rt},
		faults:  store,
		timeout: timeout,
	}
}

// GetJSON performs a GET and decodes the JSON response into out.
func (c *Client) GetJSON(ctx context.Context, path string, out any) error {
	return c.do(ctx, http.MethodGet, path, nil, out)
}

// PostJSON performs a POST with a JSON body and decodes the JSON response.
func (c *Client) PostJSON(ctx context.Context, path string, in, out any) error {
	return c.do(ctx, http.MethodPost, path, in, out)
}

func (c *Client) do(ctx context.Context, method, path string, in, out any) error {
	// The DependencyTimeoutMs fault shortens this, turning a slow dependency
	// into a hard failure that cascades upstream.
	timeout := c.timeout
	if c.faults != nil {
		timeout = c.faults.ClientTimeout(c.timeout)
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	var body io.Reader
	if in != nil {
		buf, err := json.Marshal(in)
		if err != nil {
			return fmt.Errorf("marshal request: %w", err)
		}
		body = bytes.NewReader(buf)
	}

	req, err := http.NewRequestWithContext(ctx, method, c.base+path, body)
	if err != nil {
		return fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("Accept", "application/json")
	if in != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	resp, err := c.hc.Do(req)
	if err != nil {
		// When the shortened dependencyTimeoutMs deadline is what killed the
		// call, say so by name. In the cart-timeouts scenario this is the log
		// that identifies cart-service as the slow dependency — without it,
		// checkout-api looks like the origin of the failure rather than its
		// victim.
		if c.faults != nil && errors.Is(err, context.DeadlineExceeded) {
			if ms := c.faults.Get().DependencyTimeoutMs; ms > 0 {
				c.faults.LogDependencyTimeout(c.host, time.Duration(ms)*time.Millisecond)
			}
		}
		return fmt.Errorf("%s %s: %w", method, path, err)
	}
	defer func() {
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
	}()

	if resp.StatusCode >= 400 {
		snippet, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return &StatusError{Status: resp.StatusCode, Body: string(snippet), URL: c.base + path}
	}
	if out == nil {
		return nil
	}
	if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
		return fmt.Errorf("decode response from %s: %w", path, err)
	}
	return nil
}

// peerPinner asserts server.address/server.port on the CLIENT span created by
// otelhttp, guaranteeing Causely can resolve the destination service.
type peerPinner struct {
	next  http.RoundTripper
	attrs []attribute.KeyValue
}

func (p *peerPinner) RoundTrip(r *http.Request) (*http.Response, error) {
	if span := trace.SpanFromContext(r.Context()); span.IsRecording() {
		span.SetAttributes(p.attrs...)
	}
	return p.next.RoundTrip(r)
}

// ---------------------------------------------------------------------------
// Errors and helpers
// ---------------------------------------------------------------------------

// StatusError is a non-2xx response from a downstream service.
type StatusError struct {
	Status int
	Body   string
	URL    string
}

func (e *StatusError) Error() string {
	return fmt.Sprintf("%s returned %d: %s", e.URL, e.Status, e.Body)
}

// DecodeJSON reads a JSON request body, returning a 400-mapped error on
// malformed input.
func DecodeJSON(r *http.Request, out any) error {
	dec := json.NewDecoder(io.LimitReader(r.Body, 1<<20))
	dec.DisallowUnknownFields()
	if err := dec.Decode(out); err != nil {
		return &BadRequestError{Msg: "invalid JSON body: " + err.Error()}
	}
	return nil
}

// NotFoundError makes a handler return 404.
type NotFoundError struct{ Msg string }

func (e *NotFoundError) Error() string { return e.Msg }

// BadRequestError makes a handler return 400.
type BadRequestError struct{ Msg string }

func (e *BadRequestError) Error() string { return e.Msg }

func statusFor(err error) int {
	switch e := err.(type) {
	case *NotFoundError:
		return http.StatusNotFound
	case *BadRequestError:
		return http.StatusBadRequest
	case *StatusError:
		// Propagate upstream failures as 502 so they read as dependency errors.
		if e.Status >= 500 {
			return http.StatusBadGateway
		}
		return e.Status
	}
	return http.StatusInternalServerError
}

func recordServerError(ctx context.Context, err error) {
	span := trace.SpanFromContext(ctx)
	if !span.IsRecording() {
		return
	}
	span.RecordError(err)
	// Marking the span as errored is what drives Causely's error-rate symptom.
	span.SetStatus(codes.Error, err.Error())
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

// routePath strips the method prefix from a net/http 1.22 route pattern.
func routePath(pattern string) string {
	for i := 0; i < len(pattern); i++ {
		if pattern[i] == ' ' {
			return pattern[i+1:]
		}
	}
	return pattern
}

func hostPortFromURL(raw string) (string, int) {
	trimmed := raw
	scheme := "http"
	if after, ok := stripPrefix(trimmed, "https://"); ok {
		trimmed, scheme = after, "https"
	} else if after, ok := stripPrefix(trimmed, "http://"); ok {
		trimmed = after
	}
	// Drop any path component.
	for i := 0; i < len(trimmed); i++ {
		if trimmed[i] == '/' {
			trimmed = trimmed[:i]
			break
		}
	}
	host, portStr, err := net.SplitHostPort(trimmed)
	if err != nil {
		if scheme == "https" {
			return trimmed, 443
		}
		return trimmed, 80
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		return host, 80
	}
	return host, port
}

func stripPrefix(s, prefix string) (string, bool) {
	if len(s) >= len(prefix) && s[:len(prefix)] == prefix {
		return s[len(prefix):], true
	}
	return s, false
}
