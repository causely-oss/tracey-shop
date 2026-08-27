// Package admin serves the per-pod control plane: health probes and the
// fault-injection API.
//
// This listener is deliberately NOT trace-instrumented. Causely builds topology
// from CLIENT spans, so if scenario tooling or kubelet probes were traced they
// would appear as spurious dependency edges in the demo's service graph.
package admin

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/causely-oss/tracey-shop/internal/faults"
)

// Server is the admin HTTP listener.
type Server struct {
	addr   string
	mux    *http.ServeMux
	srv    *http.Server
	ready  atomic.Bool
	faults *faults.Store
}

// New builds an admin server for the given address.
func New(addr string, store *faults.Store) *Server {
	s := &Server{
		addr:   addr,
		mux:    http.NewServeMux(),
		faults: store,
	}

	s.mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	s.mux.HandleFunc("/readyz", func(w http.ResponseWriter, _ *http.Request) {
		if !s.ready.Load() {
			writeJSON(w, http.StatusServiceUnavailable, map[string]string{"status": "starting"})
			return
		}
		writeJSON(w, http.StatusOK, map[string]string{"status": "ready"})
	})
	s.mux.HandleFunc("/admin/faults", s.handleFaults)

	s.srv = &http.Server{
		Addr:              addr,
		Handler:           s.mux,
		ReadHeaderTimeout: 5 * time.Second,
	}
	return s
}

// Handle registers an extra admin route, e.g. the load generator's rate control.
func (s *Server) Handle(pattern string, h http.HandlerFunc) {
	s.mux.HandleFunc(pattern, h)
}

// SetReady flips the readiness probe.
func (s *Server) SetReady(ready bool) { s.ready.Store(ready) }

// Start runs the listener until the context is cancelled.
func (s *Server) Start(ctx context.Context) error {
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = s.srv.Shutdown(shutdownCtx)
	}()

	slog.Info("admin listener started", slog.String("addr", s.addr))
	if err := s.srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		return err
	}
	return nil
}

func (s *Server) handleFaults(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		writeJSON(w, http.StatusOK, s.faults.Get())

	case http.MethodPost, http.MethodPut, http.MethodPatch:
		body, err := io.ReadAll(io.LimitReader(r.Body, 1<<16))
		if err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
			return
		}
		patch, err := faults.DecodePatch(body)
		if err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, s.faults.Apply(patch))

	case http.MethodDelete:
		writeJSON(w, http.StatusOK, s.faults.Clear())

	default:
		w.Header().Set("Allow", "GET, POST, PUT, PATCH, DELETE")
		writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "method not allowed"})
	}
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

// WriteJSON is exported for roles that register extra admin routes.
func WriteJSON(w http.ResponseWriter, status int, v any) { writeJSON(w, status, v) }
