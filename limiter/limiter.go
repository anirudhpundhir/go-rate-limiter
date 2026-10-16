// Package limiter defines the rate limiting contract shared by every backend.
//
// A Rule describes "Limit units per Window". Two algorithms interpret it:
//
//   - TokenBucket: a bucket of capacity Limit that refills continuously at
//     Limit/Window tokens per second. Allows bursts up to Limit.
//   - SlidingWindow: an exact sliding log; at most Limit units are admitted
//     in any interval of length Window.
package limiter

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// Algorithm selects how a Rule is enforced.
type Algorithm string

const (
	TokenBucket   Algorithm = "token_bucket"
	SlidingWindow Algorithm = "sliding_window"
)

// Rule is a limit of Limit units per Window.
type Rule struct {
	Algorithm Algorithm     `json:"algorithm"`
	Limit     int64         `json:"limit"`
	Window    time.Duration `json:"window"`
}

// ErrInvalidRule is returned for rules that cannot be enforced.
var ErrInvalidRule = errors.New("limiter: invalid rule")

// Validate checks that the rule is enforceable.
func (r Rule) Validate() error {
	if r.Algorithm != TokenBucket && r.Algorithm != SlidingWindow {
		return fmt.Errorf("%w: unknown algorithm %q", ErrInvalidRule, r.Algorithm)
	}
	if r.Limit <= 0 {
		return fmt.Errorf("%w: limit must be > 0", ErrInvalidRule)
	}
	if r.Window < time.Millisecond {
		return fmt.Errorf("%w: window must be >= 1ms", ErrInvalidRule)
	}
	return nil
}

// Decision is the outcome of a single Allow call.
type Decision struct {
	Allowed bool `json:"allowed"`
	// Remaining is the number of units still admissible right now (floored).
	Remaining int64 `json:"remaining"`
	// RetryAfter is how long to wait before the same request could succeed.
	// Zero when Allowed. For requests with n > Limit it is -1 (never).
	RetryAfter time.Duration `json:"retry_after"`
	// Source names the backend that produced the decision ("redis", "memory").
	Source string `json:"source"`
}

// Limiter admits or rejects n units for key under rule. Implementations must
// be safe for concurrent use. A rejected call consumes nothing.
type Limiter interface {
	Allow(ctx context.Context, key string, rule Rule, n int64) (Decision, error)
}

// Clock abstracts time for deterministic tests.
type Clock interface{ Now() time.Time }

// SystemClock is the wall clock.
type SystemClock struct{}

func (SystemClock) Now() time.Time { return time.Now() }

func checkArgs(rule Rule, n int64) error {
	if err := rule.Validate(); err != nil {
		return err
	}
	if n <= 0 {
		return fmt.Errorf("%w: n must be > 0", ErrInvalidRule)
	}
	return nil
}
