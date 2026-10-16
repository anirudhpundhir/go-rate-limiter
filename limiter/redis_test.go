package limiter

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func TestRedisConformanceMiniredis(t *testing.T) {
	conformance(t, backend{"miniredis", func(t *testing.T, c *fakeClock) Limiter {
		mr := miniredis.RunT(t)
		cl := redis.NewClient(&redis.Options{Addr: mr.Addr(), PoolSize: 64})
		t.Cleanup(func() { _ = cl.Close() })
		return NewRedis(cl, WithClockOverride(c))
	}})
}

// realRedis returns a client for $RATELIMIT_TEST_REDIS_ADDR or skips.
func realRedis(t testing.TB) redis.UniversalClient {
	addr := os.Getenv("RATELIMIT_TEST_REDIS_ADDR")
	if addr == "" {
		t.Skip("RATELIMIT_TEST_REDIS_ADDR not set")
	}
	cl := redis.NewClient(&redis.Options{Addr: addr, PoolSize: 128})
	if err := cl.Ping(context.Background()).Err(); err != nil {
		t.Fatalf("redis ping: %v", err)
	}
	t.Cleanup(func() { _ = cl.Close() })
	return cl
}

var prefixSeq atomic.Int64

func uniquePrefix() string {
	return fmt.Sprintf("rltest:%d:%d:", time.Now().UnixNano(), prefixSeq.Add(1))
}

func TestRedisConformanceReal(t *testing.T) {
	cl := realRedis(t)
	conformance(t, backend{"redis", func(t *testing.T, c *fakeClock) Limiter {
		return NewRedis(cl, WithClockOverride(c), WithPrefix(uniquePrefix()))
	}})
}

// Uses Redis TIME (no override): the production configuration.
func TestRedisServerClockReal(t *testing.T) {
	cl := realRedis(t)
	l := NewRedis(cl, WithPrefix(uniquePrefix()))
	r := Rule{Algorithm: SlidingWindow, Limit: 3, Window: 300 * time.Millisecond}
	for i := 0; i < 3; i++ {
		mustAllow(t, l, "k", r, 1, true)
	}
	d := mustAllow(t, l, "k", r, 1, false)
	if d.RetryAfter <= 0 || d.RetryAfter > 300*time.Millisecond {
		t.Fatalf("retry = %v", d.RetryAfter)
	}
	time.Sleep(d.RetryAfter + 20*time.Millisecond)
	mustAllow(t, l, "k", r, 1, true)
	ttl := cl.PTTL(context.Background(), l.prefix+"sw:{k}").Val()
	if ttl <= 0 || ttl > 302*time.Millisecond {
		t.Fatalf("ttl = %v, want (0, window]", ttl)
	}
}

func TestRedisScriptReloadAfterFlush(t *testing.T) {
	mr := miniredis.RunT(t)
	cl := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer cl.Close()
	l := NewRedis(cl)
	r := Rule{Algorithm: TokenBucket, Limit: 5, Window: time.Second}
	mustAllow(t, l, "k", r, 1, true)
	mr.FlushAll()
	if err := cl.ScriptFlush(context.Background()).Err(); err != nil {
		t.Fatal(err)
	}
	mustAllow(t, l, "k", r, 1, true) // NOSCRIPT -> EVAL fallback
}

func TestRedisDownReturnsError(t *testing.T) {
	mr := miniredis.RunT(t)
	cl := redis.NewClient(&redis.Options{Addr: mr.Addr(), MaxRetries: -1, DialTimeout: 50 * time.Millisecond})
	defer cl.Close()
	l := NewRedis(cl)
	mr.Close()
	_, err := l.Allow(context.Background(), "k", Rule{Algorithm: TokenBucket, Limit: 1, Window: time.Second}, 1)
	if err == nil {
		t.Fatal("expected error with redis down")
	}
	if errors.Is(err, ErrInvalidRule) {
		t.Fatal("wrong error class")
	}
}
