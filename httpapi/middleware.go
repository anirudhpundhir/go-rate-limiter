package httpapi

import (
	"log/slog"
	"math"
	"net"
	"net/http"
	"strconv"

	"github.com/anirudhpundhir/go-rate-limiter/limiter"
)

// KeyFunc derives the rate-limit key from a request. Returning "" skips limiting.
type KeyFunc func(*http.Request) string

// ByRemoteIP keys on the connection's remote IP. Behind a proxy, supply a
// KeyFunc that reads a trusted header instead.
func ByRemoteIP(r *http.Request) string {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return "ip:" + host
}

// ByHeader keys on a request header (e.g. an API key), skipping if absent.
func ByHeader(name string) KeyFunc {
	return func(r *http.Request) string {
		if v := r.Header.Get(name); v != "" {
			return "hdr:" + v
		}
		return ""
	}
}

// MiddlewareConfig configures Middleware.
type MiddlewareConfig struct {
	Limiter limiter.Limiter
	Rule    limiter.Rule
	Key     KeyFunc
	// FailClosed rejects with 503 when the limiter errors. Default is to fail
	// open (serve the request), which is what most APIs want; combine with
	// limiter.Fallback to keep limiting during a Redis outage.
	FailClosed bool
	Logger     *slog.Logger
}

// Middleware enforces cfg.Rule per key. It sets the IETF draft
// RateLimit-Limit / RateLimit-Remaining headers, and Retry-After on 429.
func Middleware(cfg MiddlewareConfig) func(http.Handler) http.Handler {
	if cfg.Key == nil {
		cfg.Key = ByRemoteIP
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	limit := strconv.FormatInt(cfg.Rule.Limit, 10)
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			key := cfg.Key(r)
			if key == "" {
				next.ServeHTTP(w, r)
				return
			}
			d, err := cfg.Limiter.Allow(r.Context(), key, cfg.Rule, 1)
			if err != nil {
				cfg.Logger.Warn("rate limiter error", "err", err, "fail_closed", cfg.FailClosed)
				if cfg.FailClosed {
					http.Error(w, "rate limiter unavailable", http.StatusServiceUnavailable)
					return
				}
				next.ServeHTTP(w, r)
				return
			}
			h := w.Header()
			h.Set("RateLimit-Limit", limit)
			h.Set("RateLimit-Remaining", strconv.FormatInt(max(d.Remaining, 0), 10))
			if !d.Allowed {
				if d.RetryAfter > 0 {
					h.Set("Retry-After", strconv.FormatInt(int64(math.Ceil(d.RetryAfter.Seconds())), 10))
				}
				http.Error(w, "rate limit exceeded", http.StatusTooManyRequests)
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}
