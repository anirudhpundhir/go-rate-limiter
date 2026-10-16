package limiter

import (
	"context"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// backend builds a fresh limiter driven by the given fake clock.
type backend struct {
	name string
	make func(t *testing.T, c *fakeClock) Limiter
}

// conformance runs the same behavioural suite against every backend, so the
// Redis Lua scripts and the in-memory implementation cannot drift apart.
func conformance(t *testing.T, b backend) {
	ctx := context.Background()
	tb := Rule{Algorithm: TokenBucket, Limit: 10, Window: time.Second}
	sw := Rule{Algorithm: SlidingWindow, Limit: 10, Window: time.Second}

	t.Run("token bucket burst then refill", func(t *testing.T) {
		c := newFakeClock()
		l := b.make(t, c)
		for i := 0; i < 10; i++ {
			mustAllow(t, l, "k", tb, 1, true)
		}
		d := mustAllow(t, l, "k", tb, 1, false)
		if d.RetryAfter != 100*time.Millisecond {
			t.Fatalf("retry after = %v, want 100ms", d.RetryAfter)
		}
		c.Advance(100 * time.Millisecond)
		mustAllow(t, l, "k", tb, 1, true)
		mustAllow(t, l, "k", tb, 1, false)
		c.Advance(10 * time.Second) // refill caps at Limit
		d = mustAllow(t, l, "k", tb, 1, true)
		if d.Remaining != 9 {
			t.Fatalf("remaining = %d, want 9", d.Remaining)
		}
	})

	t.Run("token bucket n>1 and n>limit", func(t *testing.T) {
		c := newFakeClock()
		l := b.make(t, c)
		mustAllow(t, l, "k", tb, 7, true)
		d := mustAllow(t, l, "k", tb, 4, false) // 3 left, need 1 more token
		if d.RetryAfter != 100*time.Millisecond {
			t.Fatalf("retry = %v", d.RetryAfter)
		}
		mustAllow(t, l, "k", tb, 3, true) // rejection consumed nothing
		d = mustAllow(t, l, "k", tb, 11, false)
		if d.RetryAfter != -1 {
			t.Fatalf("n>limit retry = %v, want -1", d.RetryAfter)
		}
	})

	t.Run("sliding window exact boundary", func(t *testing.T) {
		c := newFakeClock()
		l := b.make(t, c)
		for i := 0; i < 5; i++ {
			mustAllow(t, l, "k", sw, 1, true)
		}
		c.Advance(400 * time.Millisecond)
		for i := 0; i < 5; i++ {
			mustAllow(t, l, "k", sw, 1, true)
		}
		d := mustAllow(t, l, "k", sw, 1, false)
		if d.RetryAfter != 600*time.Millisecond {
			t.Fatalf("retry = %v, want 600ms", d.RetryAfter)
		}
		c.Advance(599 * time.Millisecond)
		mustAllow(t, l, "k", sw, 1, false)
		c.Advance(time.Millisecond) // first 5 now exactly Window old -> expired
		d = mustAllow(t, l, "k", sw, 5, true)
		if d.Remaining != 0 {
			t.Fatalf("remaining = %d", d.Remaining)
		}
		d = mustAllow(t, l, "k", sw, 2, false)
		if d.RetryAfter != 400*time.Millisecond {
			t.Fatalf("retry n=2 = %v, want 400ms", d.RetryAfter)
		}
	})

	t.Run("keys are independent", func(t *testing.T) {
		c := newFakeClock()
		l := b.make(t, c)
		mustAllow(t, l, "a", sw, 10, true)
		mustAllow(t, l, "a", sw, 1, false)
		mustAllow(t, l, "b", sw, 10, true)
	})

	// Property: over a random arrival schedule, the token bucket never admits
	// more than Limit + rate*elapsed, and the sliding window never admits more
	// than Limit in any Window-length interval (checked against a reference
	// computed from the admission log).
	t.Run("randomized invariants", func(t *testing.T) {
		rng := rand.New(rand.NewSource(42))
		for _, rule := range []Rule{tb, sw} {
			c := newFakeClock()
			l := b.make(t, c)
			start := c.Now()
			var admitted []time.Time
			var total int64
			for i := 0; i < 2000; i++ {
				c.Advance(time.Duration(rng.Intn(60)) * time.Millisecond)
				n := int64(1 + rng.Intn(3))
				d, err := l.Allow(ctx, "p", rule, n)
				if err != nil {
					t.Fatal(err)
				}
				if d.Allowed {
					total += n
					for j := int64(0); j < n; j++ {
						admitted = append(admitted, c.Now())
					}
				}
			}
			elapsed := c.Now().Sub(start)
			if rule.Algorithm == TokenBucket {
				bound := rule.Limit + int64(float64(rule.Limit)*elapsed.Seconds()) + 1
				if total > bound {
					t.Fatalf("token bucket admitted %d > bound %d", total, bound)
				}
			} else {
				for i := range admitted {
					j := i
					for j < len(admitted) && admitted[j].Sub(admitted[i]) < rule.Window {
						j++
					}
					if int64(j-i) > rule.Limit {
						t.Fatalf("sliding window admitted %d in one window", j-i)
					}
				}
			}
			if total == 0 {
				t.Fatal("nothing admitted")
			}
		}
	})

	// Concurrency accuracy: many goroutines hammer one key with no refill
	// (very long window). Exactly Limit units must be admitted, never more
	// (over-admission) and never fewer (lost updates in the other direction).
	t.Run("concurrent accuracy", func(t *testing.T) {
		for _, alg := range []Algorithm{TokenBucket, SlidingWindow} {
			c := newFakeClock()
			l := b.make(t, c)
			rule := Rule{Algorithm: alg, Limit: 500, Window: time.Hour}
			var allowed atomic.Int64
			var wg sync.WaitGroup
			const workers, per = 32, 50
			for w := 0; w < workers; w++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					for i := 0; i < per; i++ {
						d, err := l.Allow(ctx, "hot", rule, 1)
						if err != nil {
							t.Error(err)
							return
						}
						if d.Allowed {
							allowed.Add(1)
						}
					}
				}()
			}
			wg.Wait()
			if got := allowed.Load(); got != rule.Limit {
				t.Fatalf("%s: admitted %d of %d requests, want exactly %d", alg, got, workers*per, rule.Limit)
			}
		}
	})
}

func mustAllow(t *testing.T, l Limiter, key string, r Rule, n int64, want bool) Decision {
	t.Helper()
	d, err := l.Allow(context.Background(), key, r, n)
	if err != nil {
		t.Fatal(err)
	}
	if d.Allowed != want {
		t.Fatalf("Allow(%s,n=%d) = %v, want %v (decision %+v)", key, n, d.Allowed, want, d)
	}
	return d
}

func TestMemoryConformance(t *testing.T) {
	conformance(t, backend{"memory", func(_ *testing.T, c *fakeClock) Limiter { return NewMemory(c) }})
}

func TestValidate(t *testing.T) {
	bad := []Rule{
		{Algorithm: "x", Limit: 1, Window: time.Second},
		{Algorithm: TokenBucket, Limit: 0, Window: time.Second},
		{Algorithm: SlidingWindow, Limit: 1, Window: 0},
	}
	l := NewMemory(nil)
	for _, r := range bad {
		if _, err := l.Allow(context.Background(), "k", r, 1); err == nil {
			t.Fatalf("rule %+v accepted", r)
		}
	}
	if _, err := l.Allow(context.Background(), "k", Rule{Algorithm: TokenBucket, Limit: 1, Window: time.Second}, 0); err == nil {
		t.Fatal("n=0 accepted")
	}
}

func TestMemorySweep(t *testing.T) {
	c := newFakeClock()
	m := NewMemory(c)
	r := Rule{Algorithm: TokenBucket, Limit: 1, Window: time.Second}
	for _, k := range []string{"a", "b", "c"} {
		mustAllow(t, m, k, r, 1, true)
		mustAllow(t, m, k, Rule{Algorithm: SlidingWindow, Limit: 1, Window: time.Second}, 1, true)
	}
	if n := m.Sweep(); n != 0 {
		t.Fatalf("swept %d live keys", n)
	}
	c.Advance(2 * time.Second)
	if n := m.Sweep(); n != 6 {
		t.Fatalf("swept %d, want 6", n)
	}
}
