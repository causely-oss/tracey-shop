// Package ledger implements ledger-svc: the deepest synchronous service in the
// demo.
//
// It writes a balanced double-entry pair inside one Postgres transaction and
// publishes to the ledger.events topic. Because it is five hops from the
// storefront (web-client -> storefront -> checkout -> payment -> ledger -> Postgres),
// a slow query here is the demo's clearest test of whether Causely can point at
// the bottom of a chain rather than at the service that first reported latency.
package ledger

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	shopv1 "github.com/causely-oss/tracey-shop/gen/shop/v1"
	"github.com/causely-oss/tracey-shop/internal/app"
	"github.com/causely-oss/tracey-shop/internal/domain"
	"github.com/causely-oss/tracey-shop/internal/obs"
	"github.com/causely-oss/tracey-shop/internal/store"
	"github.com/causely-oss/tracey-shop/internal/transport/grpcx"
	"github.com/causely-oss/tracey-shop/internal/transport/kafkax"
	"github.com/causely-oss/tracey-shop/internal/transport/pgxx"
)

// Accounts used by the demo's chart of accounts.
const (
	accountAccountsReceivable = "assets:accounts_receivable"
	accountRevenue            = "revenue:sales"
)

type server struct {
	shopv1.UnimplementedLedgerServiceServer
	deps     *app.Deps
	pg       *pgxx.Pool
	producer *kafkax.Producer
}

// Run starts the ledger gRPC server.
func Run(ctx context.Context, d *app.Deps) error {
	pool, err := d.Postgres(ctx)
	if err != nil {
		return fmt.Errorf("postgres: %w", err)
	}
	if err := store.Migrate(ctx, pool); err != nil {
		return fmt.Errorf("migrate: %w", err)
	}
	// Self-heal if the bundled Postgres is ever recreated on an empty volume.
	store.StartReconciler(ctx, pool)
	producer, err := d.Producer(ctx)
	if err != nil {
		return fmt.Errorf("kafka producer: %w", err)
	}

	srv := grpcx.NewServer(d.Cfg.GRPCAddr, d.Faults)
	shopv1.RegisterLedgerServiceServer(srv.Raw(), &server{deps: d, pg: pool, producer: producer})

	d.Admin.SetReady(true)
	return srv.Start(ctx)
}

func (s *server) RecordTransaction(ctx context.Context, req *shopv1.RecordTransactionRequest) (*shopv1.RecordTransactionResponse, error) {
	if req.GetTransactionId() == "" {
		return nil, status.Error(codes.InvalidArgument, "transaction id is required")
	}
	amount := domain.MoneyFromProto(req.GetAmount())
	if amount.Cents <= 0 {
		return nil, status.Error(codes.InvalidArgument, "amount must be positive")
	}

	// Fault hooks; no-ops unless a scenario enabled them.
	s.pg.SlowDown(ctx)
	s.pg.LeakConnIfEnabled(ctx)

	journalID := domain.NewID("jrnl")

	tx, err := s.pg.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin ledger tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	// A debit and a matching credit, so the ledger always balances.
	entries := []struct {
		account   string
		direction string
	}{
		{accountAccountsReceivable, "debit"},
		{accountRevenue, "credit"},
	}
	for _, e := range entries {
		if _, err := tx.Exec(ctx, `
            INSERT INTO ledger_entries
                (journal_id, transaction_id, order_id, account, direction, amount_cents)
            VALUES ($1, $2, $3, $4, $5, $6)`,
			journalID, req.GetTransactionId(), req.GetOrderId(),
			e.account, e.direction, amount.Cents,
		); err != nil {
			return nil, fmt.Errorf("insert %s entry: %w", e.direction, err)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit ledger tx: %w", err)
	}

	// Publishing after the commit: the event describes a fact, not an intent.
	if err := s.producer.Publish(ctx, s.deps.Cfg.TopicLedgerEvents, req.GetOrderId(), domain.LedgerEvent{
		JournalID:     journalID,
		TransactionID: req.GetTransactionId(),
		OrderID:       req.GetOrderId(),
		Amount:        amount,
		RecordedAt:    time.Now().UTC(),
	}); err != nil {
		slog.Error("failed to publish ledger event",
			append(obs.LogTraceCtx(ctx),
				slog.String("journal_id", journalID),
				slog.Any("err", err))...)
	}

	return &shopv1.RecordTransactionResponse{
		JournalId:  journalID,
		EntryCount: int64(len(entries)),
	}, nil
}

func (s *server) GetBalance(ctx context.Context, req *shopv1.GetBalanceRequest) (*shopv1.GetBalanceResponse, error) {
	account := req.GetAccount()
	if account == "" {
		account = accountRevenue
	}
	s.pg.SlowDown(ctx)

	var balance int64
	err := s.pg.QueryRow(ctx, `
        SELECT COALESCE(SUM(
            CASE WHEN direction = 'debit' THEN amount_cents ELSE -amount_cents END
        ), 0)
        FROM ledger_entries
        WHERE account = $1`, account).Scan(&balance)
	if err != nil {
		return nil, fmt.Errorf("sum ledger balance: %w", err)
	}

	return &shopv1.GetBalanceResponse{
		Account: account,
		Balance: &shopv1.Money{Cents: balance, Currency: "USD"},
	}, nil
}
