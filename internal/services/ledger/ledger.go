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

// maxPostingCents is the largest amount the downstream accounting system will
// accept on a single journal line. Settlements above it are posted as several
// lines against the same journal id.
//
// It sits well above any consumer order — the catalogue tops out at $249.04 and
// a cart holds a handful of units — so an ordinary checkout is always a single
// line. Wholesale carts are case quantities and routinely clear it.
const maxPostingCents = 250_000 // $2,500

// splitPostings breaks a settlement into journal lines no larger than the
// posting limit. The lines must sum to exactly the settlement amount, or the
// journal will not balance and the period will not reconcile.
func splitPostings(cents int64) []int64 {
	if cents <= maxPostingCents {
		return []int64{cents}
	}
	lines := make([]int64, 0, cents/maxPostingCents+1)
	for remaining := cents; remaining > 0; remaining -= maxPostingCents {
		line := remaining
		if line > maxPostingCents {
			line = maxPostingCents
		}
		lines = append(lines, line)
	}
	return lines
}

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

	// Anything above the downstream system's line cap is posted as several
	// lines against this journal.
	postings := splitPostings(amount.Cents)

	var posted int64
	for _, p := range postings {
		posted += p
	}
	// Checked before the transaction is opened: a journal whose lines do not
	// sum to the authorized amount would silently corrupt the ledger, and
	// writing rows only to roll them back would put the churn on Postgres for
	// nothing.
	if posted != amount.Cents {
		slog.Error("journal entries do not balance the authorized amount, refusing to post",
			append(obs.LogTraceCtx(ctx),
				slog.String("transaction_id", req.GetTransactionId()),
				slog.String("order_id", req.GetOrderId()),
				slog.Int64("authorized_cents", amount.Cents),
				slog.Int64("posted_cents", posted),
				slog.Int("posting_lines", len(postings)),
				slog.Int64("line_cap_cents", maxPostingCents))...)
		return nil, status.Error(codes.Internal,
			"journal entries do not balance the authorized amount")
	}

	tx, err := s.pg.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin ledger tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	// A debit and a matching credit per posting line, so the ledger always
	// balances.
	entries := []struct {
		account   string
		direction string
	}{
		{accountAccountsReceivable, "debit"},
		{accountRevenue, "credit"},
	}
	for _, line := range postings {
		for _, e := range entries {
			if _, err := tx.Exec(ctx, `
            INSERT INTO ledger_entries
                (journal_id, transaction_id, order_id, account, direction, amount_cents)
            VALUES ($1, $2, $3, $4, $5, $6)`,
				journalID, req.GetTransactionId(), req.GetOrderId(),
				e.account, e.direction, line,
			); err != nil {
				return nil, fmt.Errorf("insert %s entry: %w", e.direction, err)
			}
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
		EntryCount: int64(len(entries) * len(postings)),
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
