// Package payment implements payment-gw: a gRPC service that calls an external
// processor over HTTP and records the movement in ledger-svc over gRPC.
//
// It sits at layer 3 with two downstream dependencies of different protocols,
// which makes it the best place to inject an error rate: the failure is visible
// three layers up at the storefront, and Causely has to distinguish it from its
// own dependencies' health.
package payment

import (
	"context"
	"fmt"
	"log/slog"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	shopv1 "github.com/causely-oss/tracey-shop/gen/shop/v1"
	"github.com/causely-oss/tracey-shop/internal/app"
	"github.com/causely-oss/tracey-shop/internal/domain"
	"github.com/causely-oss/tracey-shop/internal/obs"
	"github.com/causely-oss/tracey-shop/internal/transport/grpcx"
	"github.com/causely-oss/tracey-shop/internal/transport/httpx"
)

type server struct {
	shopv1.UnimplementedPaymentServiceServer
	deps      *app.Deps
	processor *httpx.Client
	ledger    shopv1.LedgerServiceClient
}

// Run starts the payment gRPC server.
func Run(ctx context.Context, d *app.Deps) error {
	ledger, err := d.LedgerClient()
	if err != nil {
		return fmt.Errorf("ledger client: %w", err)
	}

	srv := grpcx.NewServer(d.Cfg.GRPCAddr, d.Faults)
	shopv1.RegisterPaymentServiceServer(srv.Raw(), &server{
		deps:      d,
		processor: d.HTTPClient(d.Cfg.StripeURL),
		ledger:    ledger,
	})

	d.Admin.SetReady(true)
	return srv.Start(ctx)
}

func (s *server) Authorize(ctx context.Context, req *shopv1.AuthorizeRequest) (*shopv1.AuthorizeResponse, error) {
	if req.GetOrderId() == "" {
		return nil, status.Error(codes.InvalidArgument, "order id is required")
	}
	amount := domain.MoneyFromProto(req.GetAmount())
	if amount.Cents <= 0 {
		return nil, status.Error(codes.InvalidArgument, "amount must be positive")
	}

	transactionID := domain.NewID("txn")

	// 1. Charge the external processor over HTTP.
	var charge domain.PartnerResponse
	err := s.processor.PostJSON(ctx, "/charges", domain.PartnerRequest{
		Reference: transactionID,
		AmountC:   amount.Cents,
		Currency:  amount.Currency,
		Metadata: map[string]string{
			"orderId":    req.GetOrderId(),
			"customerId": req.GetCustomerId(),
			"cardBrand":  req.GetCard().GetBrand(),
			"cardLast4":  req.GetCard().GetLastFour(),
		},
	}, &charge)
	if err != nil {
		return nil, fmt.Errorf("processor charge: %w", err)
	}

	// 2. Record the double-entry movement over gRPC.
	ledgerCtx, cancel := grpcx.WithTimeout(ctx, s.deps.Faults, 2*s.deps.Cfg.RequestTimeout)
	defer cancel()

	entry, err := s.ledger.RecordTransaction(ledgerCtx, &shopv1.RecordTransactionRequest{
		TransactionId: transactionID,
		OrderId:       req.GetOrderId(),
		CustomerId:    req.GetCustomerId(),
		Amount:        req.GetAmount(),
	})
	if err != nil {
		return nil, fmt.Errorf("record ledger transaction: %w", err)
	}

	slog.Debug("payment authorized",
		append(obs.LogTraceCtx(ctx),
			slog.String("order_id", req.GetOrderId()),
			slog.String("transaction_id", transactionID),
			slog.String("journal_id", entry.GetJournalId()))...)

	return &shopv1.AuthorizeResponse{
		TransactionId:     transactionID,
		AuthorizationCode: charge.Code,
		Processor:         charge.Provider,
	}, nil
}
