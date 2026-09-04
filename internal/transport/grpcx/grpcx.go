// Package grpcx provides the demo's instrumented gRPC server and client dialer.
//
// One subtlety drives the design here. otelgrpc's client stats handler sets
// server.address from grpc's resolved peer on stats.OutHeader, which in
// Kubernetes is an IP:port rather than the Service DNS name. Causely can resolve
// either form, but the hostname path is the well-trodden one — it indexes
// Services by hostname and retries a short name as <name>.<caller namespace>.
// So peerPinHandler is registered after otelgrpc's handler and re-asserts the
// DNS target on the span, making the resolved edge deterministic.
package grpcx

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"strconv"
	"time"

	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.opentelemetry.io/otel/attribute"
	semconv "go.opentelemetry.io/otel/semconv/v1.34.0"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/stats"
	"google.golang.org/grpc/status"

	"github.com/causely-oss/tracey-shop/internal/faults"
)

// ---------------------------------------------------------------------------
// Server
// ---------------------------------------------------------------------------

// Server wraps a grpc.Server with tracing, the fault gate and a health service.
type Server struct {
	addr string
	srv  *grpc.Server
}

// NewServer builds an instrumented gRPC server. The returned *grpc.Server is
// exposed via Raw so each role can register its own service implementation.
func NewServer(addr string, store *faults.Store) *Server {
	srv := grpc.NewServer(
		grpc.StatsHandler(otelgrpc.NewServerHandler()),
		grpc.ChainUnaryInterceptor(
			recoveryInterceptor(),
			faultInterceptor(store),
		),
	)
	healthpb.RegisterHealthServer(srv, health.NewServer())
	return &Server{addr: addr, srv: srv}
}

// Raw exposes the underlying server for service registration.
func (s *Server) Raw() *grpc.Server { return s.srv }

// Start serves until the context is cancelled.
func (s *Server) Start(ctx context.Context) error {
	lis, err := net.Listen("tcp", s.addr)
	if err != nil {
		return fmt.Errorf("listen %s: %w", s.addr, err)
	}

	go func() {
		<-ctx.Done()
		stopped := make(chan struct{})
		go func() {
			s.srv.GracefulStop()
			close(stopped)
		}()
		select {
		case <-stopped:
		case <-time.After(10 * time.Second):
			s.srv.Stop()
		}
	}()

	slog.Info("grpc listener started", slog.String("addr", s.addr))
	if err := s.srv.Serve(lis); err != nil {
		return err
	}
	return nil
}

// faultInterceptor runs the fault gate before every RPC and maps an injected
// failure onto codes.Internal, which is what Causely reads from
// rpc.response.status_code to compute the service's error rate (otelgrpc v0.70
// replaced the numeric rpc.grpc.status_code with that string form; the mediator
// reads either).
func faultInterceptor(store *faults.Store) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if err := store.Gate(ctx); err != nil {
			if ctx.Err() != nil {
				return nil, status.Error(codes.DeadlineExceeded, err.Error())
			}
			return nil, status.Error(codes.Internal, err.Error())
		}
		return handler(ctx, req)
	}
}

// recoveryInterceptor keeps an injected panic from taking down unrelated RPCs
// mid-flight; the panic still propagates as a crash when panicRate is set on a
// handler goroutine outside the interceptor.
func recoveryInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp any, err error) {
		defer func() {
			if r := recover(); r != nil {
				slog.Error("panic in grpc handler",
					slog.String("method", info.FullMethod),
					slog.Any("panic", r))
				// Re-panic so the process dies and Kubernetes restarts the pod:
				// CrashLoopBackOff is the observable symptom the demo wants.
				panic(r)
			}
		}()
		return handler(ctx, req)
	}
}

// ---------------------------------------------------------------------------
// Client
// ---------------------------------------------------------------------------

// Dial opens an instrumented client connection to a host:port target.
func Dial(target string) (*grpc.ClientConn, error) {
	host, port := splitHostPort(target)
	peerAttrs := []attribute.KeyValue{
		semconv.ServerAddress(host),
		semconv.ServerPort(port),
	}

	conn, err := grpc.NewClient(target,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		// Seed the span with the DNS peer at creation time...
		grpc.WithStatsHandler(otelgrpc.NewClientHandler(
			otelgrpc.WithSpanAttributes(peerAttrs...),
		)),
		// ...then re-assert it after otelgrpc overwrites it with the peer IP.
		grpc.WithStatsHandler(&peerPinHandler{attrs: peerAttrs}),
		grpc.WithDefaultServiceConfig(`{"loadBalancingConfig":[{"round_robin":{}}]}`),
	)
	if err != nil {
		return nil, fmt.Errorf("dial %s: %w", target, err)
	}
	return conn, nil
}

// peerPinHandler pins server.address/server.port to the configured DNS target.
//
// grpc-go invokes stats handlers in registration order, and each receives the
// context returned by the preceding handler's TagRPC — so the span otelgrpc
// created is reachable here, and a later SetAttributes wins.
type peerPinHandler struct {
	attrs []attribute.KeyValue
}

func (h *peerPinHandler) TagRPC(ctx context.Context, _ *stats.RPCTagInfo) context.Context {
	return ctx
}

func (h *peerPinHandler) HandleRPC(ctx context.Context, rs stats.RPCStats) {
	// stats.End is the last event for the RPC, so pinning here reliably beats
	// otelgrpc's OutHeader write.
	switch rs.(type) {
	case *stats.OutHeader, *stats.End:
		if span := trace.SpanFromContext(ctx); span.IsRecording() {
			span.SetAttributes(h.attrs...)
		}
	}
}

func (h *peerPinHandler) TagConn(ctx context.Context, _ *stats.ConnTagInfo) context.Context {
	return ctx
}

func (h *peerPinHandler) HandleConn(context.Context, stats.ConnStats) {}

// WithTimeout applies the caller's timeout, honouring the DependencyTimeoutMs
// fault when it is set.
func WithTimeout(ctx context.Context, store *faults.Store, def time.Duration) (context.Context, context.CancelFunc) {
	timeout := def
	if store != nil {
		timeout = store.ClientTimeout(def)
	}
	return context.WithTimeout(ctx, timeout)
}

func splitHostPort(target string) (string, int) {
	host, portStr, err := net.SplitHostPort(target)
	if err != nil {
		return target, 0
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		return host, 0
	}
	return host, port
}
