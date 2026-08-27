// Package redisx provides the demo's traced Valkey/Redis client.
//
// redisotel emits db.system=redis, db.statement and server.address/server.port,
// which is what Causely uses to model the cache as a database dependency of the
// calling service.
package redisx

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/redis/go-redis/extra/redisotel/v9"
	"github.com/redis/go-redis/v9"

	"github.com/causely-oss/tracey-shop/internal/faults"
)

// Client wraps a redis client with JSON helpers and cache-fault awareness.
type Client struct {
	rdb    *redis.Client
	faults *faults.Store
}

// New connects to Valkey/Redis and installs tracing.
func New(ctx context.Context, addr string, db int, store *faults.Store) (*Client, error) {
	rdb := redis.NewClient(&redis.Options{
		Addr:         addr,
		DB:           db,
		PoolSize:     20,
		MinIdleConns: 2,
		DialTimeout:  5 * time.Second,
		ReadTimeout:  3 * time.Second,
		WriteTimeout: 3 * time.Second,
	})

	if err := redisotel.InstrumentTracing(rdb); err != nil {
		return nil, fmt.Errorf("instrument redis tracing: %w", err)
	}

	if err := waitReady(ctx, rdb); err != nil {
		_ = rdb.Close()
		return nil, err
	}

	slog.Info("redis ready", slog.String("addr", addr))
	return &Client{rdb: rdb, faults: store}, nil
}

func waitReady(ctx context.Context, rdb *redis.Client) error {
	deadline := time.Now().Add(2 * time.Minute)
	var lastErr error
	for time.Now().Before(deadline) {
		pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		err := rdb.Ping(pingCtx).Err()
		cancel()
		if err == nil {
			return nil
		}
		lastErr = err
		slog.Info("waiting for redis", slog.Any("err", err))
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(3 * time.Second):
		}
	}
	return fmt.Errorf("redis not ready: %w", lastErr)
}

// Raw exposes the underlying client.
func (c *Client) Raw() *redis.Client { return c.rdb }

// Close shuts the client down.
func (c *Client) Close() error { return c.rdb.Close() }

// GetJSON reads and decodes a cached JSON value. It reports found=false on a
// cache miss, and always misses while the disableCache fault is active, which
// shifts read load onto Postgres.
func (c *Client) GetJSON(ctx context.Context, key string, out any) (bool, error) {
	if c.faults != nil && c.faults.CacheDisabled() {
		// A real cache-degradation log: it explains why read load has moved to
		// the origin, which is the only signal this scenario produces.
		c.faults.LogCacheBypass(key)
		return false, nil
	}
	raw, err := c.rdb.Get(ctx, key).Bytes()
	if err == redis.Nil {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("redis get %s: %w", key, err)
	}
	if err := json.Unmarshal(raw, out); err != nil {
		// Treat an unreadable entry as a miss rather than an error.
		return false, nil
	}
	return true, nil
}

// SetJSON writes a JSON value with a TTL.
func (c *Client) SetJSON(ctx context.Context, key string, v any, ttl time.Duration) error {
	raw, err := json.Marshal(v)
	if err != nil {
		return fmt.Errorf("marshal cache value: %w", err)
	}
	if err := c.rdb.Set(ctx, key, raw, ttl).Err(); err != nil {
		return fmt.Errorf("redis set %s: %w", key, err)
	}
	return nil
}

// Del removes a key.
func (c *Client) Del(ctx context.Context, key string) error {
	return c.rdb.Del(ctx, key).Err()
}

// IncrBy increments a counter, used by the risk feature store.
func (c *Client) IncrBy(ctx context.Context, key string, n int64, ttl time.Duration) (int64, error) {
	v, err := c.rdb.IncrBy(ctx, key, n).Result()
	if err != nil {
		return 0, fmt.Errorf("redis incrby %s: %w", key, err)
	}
	if ttl > 0 {
		_ = c.rdb.Expire(ctx, key, ttl).Err()
	}
	return v, nil
}

// HGetAllInt reads a hash of integer features.
func (c *Client) HGetAllInt(ctx context.Context, key string) (map[string]string, error) {
	m, err := c.rdb.HGetAll(ctx, key).Result()
	if err != nil {
		return nil, fmt.Errorf("redis hgetall %s: %w", key, err)
	}
	return m, nil
}

// HSet writes hash fields.
func (c *Client) HSet(ctx context.Context, key string, values ...any) error {
	if err := c.rdb.HSet(ctx, key, values...).Err(); err != nil {
		return fmt.Errorf("redis hset %s: %w", key, err)
	}
	return nil
}
