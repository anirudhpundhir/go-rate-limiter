package limiter

import (
	"context"
	"errors"
	"sync"
	"time"
)

// Fallback consults Primary (normally Redis) and, if it errors, answers from
// Secondary (normally Memory) instead of failing the request. After a primary
// failure it stops calling the primary for Cooldown, so a dead Redis does not
// add a timeout to every request (a minimal circuit breaker).
//
// Semantics while degraded: limits are enforced per process, not globally.
// Configure LocalScale (e.g. 1/N for N replicas) to approximate the global
// limit; see docs/DESIGN.md ADR-004.
type Fallback struct {
	Primary   Limiter
	Secondary Limiter
	Cooldown  time.Duration
	// LocalScale multiplies rule.Limit when answering from Secondary
	// (0 means 1.0). Values are floored and clamped to >= 1.
	LocalScale float64
	// OnFallback, if set, is called with the primary error (metrics hook).
	OnFallback func(error)
	clock      Clock

	mu        sync.Mutex
	openUntil time.Time
}

// NewFallback returns a Fallback with a 1s cooldown.
func NewFallback(primary, secondary Limiter) *Fallback {
	return &Fallback{Primary: primary, Secondary: secondary, Cooldown: time.Second, clock: SystemClock{}}
}

// Allow implements Limiter.
func (f *Fallback) Allow(ctx context.Context, key string, rule Rule, n int64) (Decision, error) {
	if err := checkArgs(rule, n); err != nil {
		return Decision{}, err
	}
	clock := f.clock
	if clock == nil {
		clock = SystemClock{}
	}
	f.mu.Lock()
	open := clock.Now().Before(f.openUntil)
	f.mu.Unlock()
	if !open {
		d, err := f.Primary.Allow(ctx, key, rule, n)
		if err == nil {
			return d, nil
		}
		if errors.Is(err, ErrInvalidRule) || ctx.Err() != nil {
			return d, err
		}
		f.mu.Lock()
		f.openUntil = clock.Now().Add(f.Cooldown)
		f.mu.Unlock()
		if f.OnFallback != nil {
			f.OnFallback(err)
		}
	}
	local := rule
	if f.LocalScale > 0 {
		local.Limit = int64(float64(rule.Limit) * f.LocalScale)
		if local.Limit < 1 {
			local.Limit = 1
		}
	}
	return f.Secondary.Allow(ctx, key, local, n)
}
