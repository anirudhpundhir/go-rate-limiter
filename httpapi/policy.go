// Package httpapi exposes limiters over HTTP: a net/http middleware and a
// JSON check API for services that are not written in Go.
package httpapi

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/anirudhpundhir/go-rate-limiter/limiter"
)

// ParsePolicy parses "name=algorithm:limit:window", e.g.
// "api=token_bucket:100:1m" or "login=sliding_window:5:15m".
func ParsePolicy(s string) (string, limiter.Rule, error) {
	name, spec, ok := strings.Cut(s, "=")
	parts := strings.Split(spec, ":")
	if !ok || name == "" || len(parts) != 3 {
		return "", limiter.Rule{}, fmt.Errorf("policy %q: want name=algorithm:limit:window", s)
	}
	limit, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil {
		return "", limiter.Rule{}, fmt.Errorf("policy %q: limit: %w", s, err)
	}
	window, err := time.ParseDuration(parts[2])
	if err != nil {
		return "", limiter.Rule{}, fmt.Errorf("policy %q: window: %w", s, err)
	}
	r := limiter.Rule{Algorithm: limiter.Algorithm(parts[0]), Limit: limit, Window: window}
	if err := r.Validate(); err != nil {
		return "", limiter.Rule{}, fmt.Errorf("policy %q: %w", s, err)
	}
	return name, r, nil
}
