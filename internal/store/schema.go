// Package store owns the Postgres schema and seed data.
//
// Migrations are idempotent and guarded by a session advisory lock, so every
// Postgres-using service can safely run them at startup without a separate
// migration Job or an ordering constraint between pods.
package store

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/causely-oss/tracey-shop/internal/transport/pgxx"
)

// advisoryLockID is an arbitrary constant unique to this application.
const advisoryLockID int64 = 8172634509

const schemaDDL = `
CREATE TABLE IF NOT EXISTS products (
    id          text PRIMARY KEY,
    sku         text NOT NULL UNIQUE,
    name        text NOT NULL,
    category    text NOT NULL,
    price_cents bigint NOT NULL,
    currency    text NOT NULL DEFAULT 'USD'
);
CREATE INDEX IF NOT EXISTS products_category_idx ON products (category);

CREATE TABLE IF NOT EXISTS inventory (
    product_id text PRIMARY KEY REFERENCES products (id) ON DELETE CASCADE,
    available  integer NOT NULL DEFAULT 0,
    reserved   integer NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS price_rules (
    id           serial PRIMARY KEY,
    category     text NOT NULL,
    customer_tier text NOT NULL,
    discount_bps integer NOT NULL,
    description  text NOT NULL,
    active       boolean NOT NULL DEFAULT true
);
CREATE INDEX IF NOT EXISTS price_rules_lookup_idx ON price_rules (category, customer_tier) WHERE active;

CREATE TABLE IF NOT EXISTS orders (
    id          text PRIMARY KEY,
    customer_id text NOT NULL,
    status      text NOT NULL,
    total_cents bigint NOT NULL,
    currency    text NOT NULL DEFAULT 'USD',
    created_at  timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS orders_created_at_idx ON orders (created_at DESC);

CREATE TABLE IF NOT EXISTS order_items (
    order_id   text NOT NULL REFERENCES orders (id) ON DELETE CASCADE,
    product_id text NOT NULL,
    quantity   integer NOT NULL,
    PRIMARY KEY (order_id, product_id)
);

CREATE TABLE IF NOT EXISTS stock_reservations (
    id         text PRIMARY KEY,
    order_id   text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS ledger_entries (
    id             bigserial PRIMARY KEY,
    journal_id     text NOT NULL,
    transaction_id text NOT NULL,
    order_id       text NOT NULL,
    account        text NOT NULL,
    direction      text NOT NULL CHECK (direction IN ('debit', 'credit')),
    amount_cents   bigint NOT NULL,
    created_at     timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS ledger_entries_journal_idx ON ledger_entries (journal_id);
CREATE INDEX IF NOT EXISTS ledger_entries_account_idx ON ledger_entries (account);
`

// seedProducts generates a deterministic catalogue. Deterministic matters: the
// load generator picks product ids by index, so every replica and every restart
// agrees on what exists and the clean baseline never 404s.
const seedProducts = `
INSERT INTO products (id, sku, name, category, price_cents, currency)
SELECT
    'P' || lpad(g::text, 4, '0'),
    'SKU-' || upper(c.category) || '-' || lpad(g::text, 4, '0'),
    c.adjective || ' ' || c.noun || ' ' || lpad(g::text, 4, '0'),
    c.category,
    ((g * 737) % 24000) + 999,
    'USD'
FROM generate_series(1, 200) AS g
CROSS JOIN LATERAL (
    SELECT
        (ARRAY['electronics','apparel','home','outdoors','books','toys','grocery','beauty'])[1 + (g % 8)] AS category,
        (ARRAY['Compact','Premium','Everyday','Rugged','Classic','Modern','Deluxe','Essential'])[1 + (g % 8)] AS adjective,
        (ARRAY['Headphones','Jacket','Lamp','Backpack','Notebook','Puzzle','Coffee','Serum'])[1 + (g % 8)] AS noun
) AS c
ON CONFLICT (id) DO NOTHING;

INSERT INTO inventory (product_id, available, reserved)
SELECT id, 500 + ((('x' || substr(md5(id), 1, 8))::bit(32)::bigint % 500)), 0
FROM products
ON CONFLICT (product_id) DO NOTHING;
`

const seedPriceRules = `
INSERT INTO price_rules (category, customer_tier, discount_bps, description)
SELECT * FROM (VALUES
    ('electronics', 'standard', 0,    'no standard discount on electronics'),
    ('electronics', 'gold',     500,  'gold 5% off electronics'),
    ('electronics', 'platinum', 800,  'platinum 8% off electronics'),
    ('apparel',     'standard', 200,  'seasonal 2% off apparel'),
    ('apparel',     'gold',     700,  'gold 7% off apparel'),
    ('apparel',     'platinum', 1000, 'platinum 10% off apparel'),
    ('home',        'standard', 0,    'no standard discount on home'),
    ('home',        'gold',     400,  'gold 4% off home'),
    ('home',        'platinum', 600,  'platinum 6% off home'),
    ('outdoors',    'standard', 100,  'clearance 1% off outdoors'),
    ('outdoors',    'gold',     500,  'gold 5% off outdoors'),
    ('outdoors',    'platinum', 750,  'platinum 7.5% off outdoors'),
    ('books',       'standard', 300,  'always 3% off books'),
    ('books',       'gold',     600,  'gold 6% off books'),
    ('books',       'platinum', 900,  'platinum 9% off books'),
    ('toys',        'standard', 0,    'no standard discount on toys'),
    ('toys',        'gold',     450,  'gold 4.5% off toys'),
    ('toys',        'platinum', 700,  'platinum 7% off toys'),
    ('grocery',     'standard', 150,  'basket 1.5% off grocery'),
    ('grocery',     'gold',     300,  'gold 3% off grocery'),
    ('grocery',     'platinum', 450,  'platinum 4.5% off grocery'),
    ('beauty',      'standard', 0,    'no standard discount on beauty'),
    ('beauty',      'gold',     550,  'gold 5.5% off beauty'),
    ('beauty',      'platinum', 850,  'platinum 8.5% off beauty')
) AS v (category, customer_tier, discount_bps, description)
WHERE NOT EXISTS (SELECT 1 FROM price_rules);
`

// sentinelTable is probed to decide whether the schema is present. Any of the
// application tables would do; products is the one every read path needs.
const sentinelTable = "products"

// reconcileInterval is how often each Postgres-using service checks that the
// schema still exists.
const reconcileInterval = 30 * time.Second

// SchemaPresent reports whether the application schema exists. The probe is a
// catalog lookup, so it is cheap enough to run on a short timer.
func SchemaPresent(ctx context.Context, pool *pgxx.Pool) (bool, error) {
	var present bool
	err := pool.QueryRow(ctx, `
        SELECT EXISTS (
            SELECT 1 FROM pg_class
            WHERE relname = $1 AND relnamespace = 'public'::regnamespace
        )`, sentinelTable).Scan(&present)
	if err != nil {
		return false, fmt.Errorf("probe schema: %w", err)
	}
	return present, nil
}

// StartReconciler re-applies the schema if it ever disappears.
//
// This exists because the bundled Postgres runs on an emptyDir by default, so
// any restart of that pod — a node drain, an eviction, a chart upgrade that
// changes the StatefulSet — starts with an empty database. Migrate only runs at
// service startup, so without this the application keeps serving against a
// database with no tables and every query fails until someone restarts every
// Postgres-using pod by hand. That is indistinguishable from a real incident in
// Causely and quietly destroys the clean baseline.
//
// Safe to run from several services at once: Migrate takes a session advisory
// lock, and all its DDL is idempotent.
func StartReconciler(ctx context.Context, pool *pgxx.Pool) {
	go func() {
		ticker := time.NewTicker(reconcileInterval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				present, err := SchemaPresent(ctx, pool)
				if err != nil {
					// Postgres being briefly unreachable is not a schema
					// problem; the next tick retries.
					slog.Debug("schema probe failed", slog.Any("err", err))
					continue
				}
				if present {
					continue
				}
				slog.Warn("application schema is missing, reapplying it",
					slog.String("sentinel_table", sentinelTable))
				if err := Migrate(ctx, pool); err != nil {
					slog.Error("failed to reapply schema", slog.Any("err", err))
					continue
				}
				slog.Info("application schema reapplied")
			}
		}
	}()
}

// Restocking.
//
// ReserveStock decrements `available` on every order and nothing ever put it
// back, so the demo quietly destroyed itself: after ~49k orders, 162 of 200
// products sat at zero, CheckStock failed for almost every cart, and checkout
// returned 500 on ~100% of attempts.
//
// The damage was worse than "checkout is broken". Because checkout fails at the
// stock check — step 2 of 8 — it never reaches payment-gw, so the payment-errors
// scenario stopped producing any payment signal at all: Causely saw payment's
// error rate at ~2.4% instead of 35%, below its ~2% symptom threshold, and put
// the root cause on checkout instead. A depleted warehouse silently invalidated
// every scenario that runs through checkout.
const (
	// restockLowWater is the per-product level that triggers a top-up.
	restockLowWater = 200
	// restockTarget is the level a restocked product returns to.
	restockTarget = 1000
	// restockInterval is how often stock is topped up. Frequent enough that a
	// long-running demo never runs dry, cheap enough to be invisible.
	restockInterval = 2 * time.Minute
)

const restockSQL = `
UPDATE inventory
SET available = $1,
    -- reserved grows without bound otherwise; it reached 147k in a day and is
    -- not read by anything, so a restock resets it.
    reserved = 0
WHERE available < $2
`

// StartRestocker keeps the warehouse stocked so the checkout path stays
// exercisable indefinitely.
//
// Runs from inventory-svc, which owns the inventory table. Idempotent and safe
// to run from multiple replicas.
func StartRestocker(ctx context.Context, pool *pgxx.Pool) {
	go func() {
		ticker := time.NewTicker(restockInterval)
		defer ticker.Stop()

		restock := func() {
			tag, err := pool.Exec(ctx, restockSQL, restockTarget, restockLowWater)
			if err != nil {
				slog.Debug("restock failed", slog.Any("err", err))
				return
			}
			if n := tag.RowsAffected(); n > 0 {
				slog.Info("restocked products",
					slog.Int64("products", n),
					slog.Int("to_level", restockTarget),
					slog.Int("low_water", restockLowWater))
			}
		}

		// Top up immediately: a service starting against an already-drained
		// database should not wait a full interval before checkout works.
		restock()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				restock()
			}
		}
	}()
}

// Migrate creates the schema and seeds reference data. Safe to call from every
// service on every start.
func Migrate(ctx context.Context, pool *pgxx.Pool) error {
	conn, err := pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("acquire migration conn: %w", err)
	}
	defer conn.Release()

	// Serialise concurrent migrations across pods.
	if _, err := conn.Exec(ctx, "SELECT pg_advisory_lock($1)", advisoryLockID); err != nil {
		return fmt.Errorf("acquire advisory lock: %w", err)
	}
	defer func() {
		_, _ = conn.Exec(ctx, "SELECT pg_advisory_unlock($1)", advisoryLockID)
	}()

	// Ordered: the schema must exist before anything is seeded into it.
	steps := []struct {
		name string
		stmt string
	}{
		{"schema", schemaDDL},
		{"products", seedProducts},
		{"price_rules", seedPriceRules},
	}
	for _, step := range steps {
		if _, err := conn.Exec(ctx, step.stmt); err != nil {
			return fmt.Errorf("apply %s: %w", step.name, err)
		}
	}

	var products, rules int
	if err := conn.QueryRow(ctx,
		"SELECT (SELECT count(*) FROM products), (SELECT count(*) FROM price_rules)",
	).Scan(&products, &rules); err != nil {
		return fmt.Errorf("verify seed: %w", err)
	}

	slog.Info("schema ready",
		slog.Int("products", products),
		slog.Int("price_rules", rules))
	return nil
}
