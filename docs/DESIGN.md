# Design notes (ADR log)

Short Architecture Decision Records. Each has context, the decision, and its consequences.

---

## ADR-001: One `Rule` (Limit per Window), two algorithms

**Context.** Callers think in "100 requests per minute". Different endpoints want different burst behaviour.

**Decision.** A `Rule{Algorithm, Limit, Window}` is interpreted by:
- **Token bucket**: capacity `Limit`, continuous refill of `Limit/Window`. Allows a full burst of `Limit` at once, then a smooth rate. Good for general API traffic.
- **Sliding window log**: at most `Limit` units in *any* interval of length `Window` (exact, not the two-bucket approximation). Good for abuse-sensitive endpoints (login, OTP, card testing) where "exactly 5 per 15 minutes" matters.

Rejected calls consume nothing. `n > Limit` can never succeed and returns `RetryAfter = -1`.

**Consequences.** The sliding log stores one entry per admitted unit, so memory is `O(Limit)` per key. That's fine for small limits (login: 5), wrong for "10k/s". Use the token bucket there, or see follow-ups (sliding window *counter*).

---

## ADR-002: Redis atomicity via Lua, with Redis as the clock

**Context.** A distributed limiter's correctness is the read-modify-write on shared state. `GET` + `SET` from N app nodes races; `WATCH/MULTI` retries under contention.

**Decision.**
- Each decision is **one Lua script** (`EVALSHA`, transparent `EVAL` on `NOSCRIPT`). Redis runs scripts atomically, so there are no lost updates and no over-admission. Verified by the concurrent-accuracy test: 32 goroutines × 50 calls on one key with `Limit=500` must admit **exactly** 500, against miniredis *and* real Redis.
- `now` comes from `redis.call('TIME')` inside the script, **not** from the app node. Clock skew between app nodes therefore can't widen the limit. (Redis ≥ 5 replicates script *effects*, so non-deterministic `TIME` is allowed.) Tests can inject time (`WithClockOverride`), which makes the Lua and Go implementations deterministically comparable.
- Keys are `prefix + "tb:{" + key + "}"`. Each script touches one key, so it is Cluster-safe.
- `PEXPIRE ≈ Window`. After one full window, both algorithms' state is equivalent to "no state", so idle keys cost nothing.
- Token counts are stored with `%.17g`, so float state round-trips losslessly.

**Consequences.** One RTT per decision (measured ~11–13 µs/op over loopback, see README). Throughput of a single hot key is bounded by single-threaded Redis. Failover with async replication can lose recent increments, which means a brief **over**-admission and never a false rejection. That's acceptable for rate limiting.

---

## ADR-003: Identical behaviour across backends: one conformance suite

**Context.** Two implementations of the same algorithm (Go, Lua) will drift.

**Decision.** `conformance_test.go` is a backend-agnostic suite: exact boundaries, `RetryAfter` values, `n>1`, key isolation, a randomized property test (token bucket admits ≤ `Limit + rate·t`; sliding window never admits > `Limit` in any window, checked against the admission log), and concurrent exact admission. It runs against memory, miniredis, and real Redis (`RATELIMIT_TEST_REDIS_ADDR`, set in CI).

---

## ADR-004: Redis outage: local fallback with a breaker, fail-open by default

**Context.** The rate limiter shouldn't become the outage. But turning limiting off entirely during a Redis outage invites abuse exactly when you're degraded.

**Decision.**
- `Fallback{Primary: Redis, Secondary: Memory}`. On a primary error, answer from the in-process limiter, and skip the primary for `Cooldown` (default 1s) so a dead Redis doesn't add a timeout to every call. Invalid-rule errors and caller cancellation are not masked.
- While degraded, limits are per process. `LocalScale` (e.g. `1/replicas`) approximates the global limit.
- Middleware fails **open** on errors by default (`FailClosed` is opt-in). With `Fallback` in front, errors are rare anyway.

**Consequences.** During an outage the effective limit is approximate (`replicas × LocalScale × Limit`), and state doesn't migrate back: on recovery, Redis state is whatever it was. A dial-refused Redis took ~3s to trip on the first request in a manual test, because go-redis retries dials inside its pool. Production configs should set tight `DialTimeout`/`ReadTimeout` (the daemon defaults to 50ms) and `MaxRetries`.

---

## ADR-005: HTTP check API with server-side policies (no gRPC yet)

**Context.** Non-Go services need to ask "may I?". Letting clients send arbitrary limits would let any client raise its own limit.

**Decision.** `POST /v1/check {policy, key, cost}`. Policies are named and configured on the server (`-policy login=sliding_window:5:15m`). The response is always 200 with `allowed`, so callers branch on the body. 4xx/5xx mean the check itself failed. The middleware emits `RateLimit-Limit`, `RateLimit-Remaining` and `Retry-After`.

**Consequences.** gRPC is deliberately deferred (follow-up). The JSON API is trivially curl-able, and its per-call overhead is dominated by the Redis RTT anyway.

---

## Follow-ups / known scope cuts

- **gRPC API** (the task allowed either; HTTP was chosen). Would mirror `CheckRequest`.
- **Sliding window counter** (two fixed buckets, weighted) for `O(1)` memory with large limits.
- **Batching / local token leasing** for hot keys: take N tokens from Redis at once and spend them locally. That trades accuracy for fewer RTTs.
- Prometheus metrics (allowed/denied/fallback counters; the `OnFallback` hook exists).
- Redis Cluster integration test (keys are hash-tagged, but this is only tested against a single node).
- Dockerfile/compose. Docker was unavailable while building this, so nothing was shipped that couldn't be verified.
- The memory backend's sliding log uses `append`+reslice, so a hot key's backing array is only reclaimed when the key is swept.
