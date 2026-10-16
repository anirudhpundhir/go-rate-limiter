package limiter

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

// Both scripts read the clock from Redis (TIME) so that every application
// node agrees on "now" regardless of local clock skew. ARGV[now] may override
// it (tests only). Scripts run atomically: no other command interleaves.

// KEYS[1]=bucket  ARGV: limit, window_us, n, now_us_override
var tokenBucketScript = redis.NewScript(`
local limit  = tonumber(ARGV[1])
local window = tonumber(ARGV[2])
local n      = tonumber(ARGV[3])
local now
if ARGV[4] ~= "" then now = tonumber(ARGV[4]) else
  local t = redis.call('TIME'); now = tonumber(t[1]) * 1000000 + tonumber(t[2]) end
local rate = limit / window -- tokens per microsecond
local h = redis.call('HMGET', KEYS[1], 'tk', 'ts')
local tokens = tonumber(h[1]); local ts = tonumber(h[2])
if tokens == nil then tokens = limit; ts = now end
if now > ts then tokens = math.min(limit, tokens + (now - ts) * rate); ts = now end
local allowed = 0; local retry = 0
if n > limit then retry = -1
elseif tokens >= n then tokens = tokens - n; allowed = 1
else retry = math.ceil((n - tokens) / rate) end
redis.call('HSET', KEYS[1], 'tk', string.format('%.17g', tokens), 'ts', ts)
redis.call('PEXPIRE', KEYS[1], math.ceil(window / 1000) + 1)
return {allowed, math.floor(tokens), retry}
`)

// KEYS[1]=zset log  ARGV: limit, window_us, n, now_us_override, nonce
var slidingWindowScript = redis.NewScript(`
local limit  = tonumber(ARGV[1])
local window = tonumber(ARGV[2])
local n      = tonumber(ARGV[3])
local now
if ARGV[4] ~= "" then now = tonumber(ARGV[4]) else
  local t = redis.call('TIME'); now = tonumber(t[1]) * 1000000 + tonumber(t[2]) end
redis.call('ZREMRANGEBYSCORE', KEYS[1], '-inf', now - window)
local count = redis.call('ZCARD', KEYS[1])
local allowed = 0; local retry = 0
if n > limit then retry = -1
elseif count + n <= limit then
  for i = 1, n do redis.call('ZADD', KEYS[1], now, ARGV[5] .. ':' .. i) end
  count = count + n; allowed = 1
else
  local need = count + n - limit
  local e = redis.call('ZRANGE', KEYS[1], need - 1, need - 1, 'WITHSCORES')
  retry = tonumber(e[2]) + window - now
end
redis.call('PEXPIRE', KEYS[1], math.ceil(window / 1000) + 1)
return {allowed, limit - count, retry}
`)

// Redis is a distributed limiter: every decision is one atomic Lua script
// execution (EVALSHA, falling back to EVAL on NOSCRIPT).
type Redis struct {
	client redis.Scripter
	prefix string
	// nowOverride, if set, is passed to the scripts instead of Redis TIME.
	nowOverride func() time.Time
}

// RedisOption configures a Redis limiter.
type RedisOption func(*Redis)

// WithPrefix namespaces all keys (default "rl:").
func WithPrefix(p string) RedisOption { return func(r *Redis) { r.prefix = p } }

// WithClockOverride makes scripts use the given clock instead of Redis TIME.
// Intended for deterministic tests.
func WithClockOverride(c Clock) RedisOption {
	return func(r *Redis) { r.nowOverride = c.Now }
}

// NewRedis builds a Redis-backed limiter over any go-redis client
// (single node, Sentinel, or Cluster; keys are single-slot by construction).
func NewRedis(client redis.Scripter, opts ...RedisOption) *Redis {
	r := &Redis{client: client, prefix: "rl:"}
	for _, o := range opts {
		o(r)
	}
	return r
}

// Allow implements Limiter.
func (r *Redis) Allow(ctx context.Context, key string, rule Rule, n int64) (Decision, error) {
	if err := checkArgs(rule, n); err != nil {
		return Decision{}, err
	}
	now := ""
	if r.nowOverride != nil {
		now = strconv.FormatInt(r.nowOverride().UnixMicro(), 10)
	}
	windowUs := rule.Window.Microseconds()
	var (
		res []interface{}
		err error
	)
	switch rule.Algorithm {
	case TokenBucket:
		res, err = tokenBucketScript.Run(ctx, r.client,
			[]string{r.prefix + "tb:{" + key + "}"},
			rule.Limit, windowUs, n, now).Slice()
	default:
		res, err = slidingWindowScript.Run(ctx, r.client,
			[]string{r.prefix + "sw:{" + key + "}"},
			rule.Limit, windowUs, n, now, nonce()).Slice()
	}
	if err != nil {
		return Decision{}, fmt.Errorf("limiter: redis: %w", err)
	}
	if len(res) != 3 {
		return Decision{}, fmt.Errorf("limiter: redis: unexpected reply %v", res)
	}
	allowed, _ := res[0].(int64)
	remaining, _ := res[1].(int64)
	retry, _ := res[2].(int64)
	d := Decision{Allowed: allowed == 1, Remaining: remaining, Source: "redis"}
	if retry < 0 {
		d.RetryAfter = -1
	} else {
		d.RetryAfter = time.Duration(retry) * time.Microsecond
	}
	return d, nil
}

func nonce() string {
	var b [12]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}
