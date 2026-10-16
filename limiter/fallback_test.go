package limiter

import (
	"context"
	"errors"
	"testing"
	"time"
)

type flaky struct {
	err   error
	calls int
}

func (f *flaky) Allow(context.Context, string, Rule, int64) (Decision, error) {
	f.calls++
	if f.err != nil {
		return Decision{}, f.err
	}
	return Decision{Allowed: true, Source: "primary"}, nil
}

func TestFallbackUsesSecondaryAndCoolsDown(t *testing.T) {
	c := newFakeClock()
	p := &flaky{err: errors.New("connection refused")}
	f := NewFallback(p, NewMemory(c))
	f.clock = c
	f.LocalScale = 0.5
	var fallbacks int
	f.OnFallback = func(error) { fallbacks++ }
	r := Rule{Algorithm: SlidingWindow, Limit: 4, Window: time.Hour}

	mustAllow(t, f, "k", r, 1, true)
	d := mustAllow(t, f, "k", r, 1, true)
	if d.Source != "memory" {
		t.Fatalf("source = %s", d.Source)
	}
	mustAllow(t, f, "k", r, 1, false) // local limit = 4*0.5 = 2
	if p.calls != 1 || fallbacks != 1 {
		t.Fatalf("primary calls=%d fallbacks=%d; breaker should be open", p.calls, fallbacks)
	}
	p.err = nil
	c.Advance(time.Second)
	d = mustAllow(t, f, "k", r, 1, true)
	if d.Source != "primary" || p.calls != 2 {
		t.Fatalf("did not recover to primary: %+v calls=%d", d, p.calls)
	}
}

func TestFallbackDoesNotMaskInvalidRule(t *testing.T) {
	f := NewFallback(&flaky{}, NewMemory(nil))
	if _, err := f.Allow(context.Background(), "k", Rule{}, 1); !errors.Is(err, ErrInvalidRule) {
		t.Fatalf("err = %v", err)
	}
}
