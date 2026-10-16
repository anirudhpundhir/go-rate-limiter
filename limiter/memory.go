package limiter

import (
	"context"
	"hash/maphash"
	"math"
	"sync"
	"time"
)

const shardCount = 64

// Memory is an in-process limiter. State is sharded by key hash to reduce
// lock contention. It is exact for a single process and is used both as a
// standalone limiter and as the fallback when Redis is unavailable.
type Memory struct {
	clock  Clock
	seed   maphash.Seed
	shards [shardCount]shard
}

type shard struct {
	mu      sync.Mutex
	buckets map[string]*bucketState
	logs    map[string]*logState
}

type bucketState struct {
	tokens  float64
	last    time.Time
	expires time.Time
}

type logState struct {
	// ring of admission timestamps (one entry per unit), oldest first
	ts      []time.Time
	expires time.Time
}

// NewMemory returns an in-memory limiter. A nil clock uses the system clock.
func NewMemory(clock Clock) *Memory {
	if clock == nil {
		clock = SystemClock{}
	}
	m := &Memory{clock: clock, seed: maphash.MakeSeed()}
	for i := range m.shards {
		m.shards[i].buckets = map[string]*bucketState{}
		m.shards[i].logs = map[string]*logState{}
	}
	return m
}

func (m *Memory) shard(key string) *shard {
	return &m.shards[maphash.String(m.seed, key)%shardCount]
}

// Allow implements Limiter.
func (m *Memory) Allow(_ context.Context, key string, rule Rule, n int64) (Decision, error) {
	if err := checkArgs(rule, n); err != nil {
		return Decision{}, err
	}
	s := m.shard(key)
	now := m.clock.Now()
	s.mu.Lock()
	defer s.mu.Unlock()
	switch rule.Algorithm {
	case TokenBucket:
		return s.tokenBucket(key, rule, n, now), nil
	default:
		return s.slidingWindow(key, rule, n, now), nil
	}
}

func (s *shard) tokenBucket(key string, rule Rule, n int64, now time.Time) Decision {
	ratePerNs := float64(rule.Limit) / float64(rule.Window)
	b, ok := s.buckets[key]
	if !ok || now.After(b.expires) {
		b = &bucketState{tokens: float64(rule.Limit), last: now}
		s.buckets[key] = b
	}
	if el := now.Sub(b.last); el > 0 {
		b.tokens = math.Min(float64(rule.Limit), b.tokens+float64(el)*ratePerNs)
		b.last = now
	}
	// A full refill takes Window; after that the state is indistinguishable
	// from a fresh bucket, so it can be dropped.
	b.expires = now.Add(rule.Window)
	d := Decision{Source: "memory"}
	if n > rule.Limit {
		d.RetryAfter = -1
	} else if b.tokens >= float64(n) {
		b.tokens -= float64(n)
		d.Allowed = true
	} else {
		d.RetryAfter = time.Duration(math.Ceil((float64(n) - b.tokens) / ratePerNs))
	}
	d.Remaining = int64(math.Floor(b.tokens))
	return d
}

func (s *shard) slidingWindow(key string, rule Rule, n int64, now time.Time) Decision {
	l, ok := s.logs[key]
	if !ok {
		l = &logState{}
		s.logs[key] = l
	}
	cutoff := now.Add(-rule.Window)
	i := 0
	for i < len(l.ts) && !l.ts[i].After(cutoff) {
		i++
	}
	l.ts = l.ts[i:]
	count := int64(len(l.ts))
	d := Decision{Source: "memory"}
	switch {
	case n > rule.Limit:
		d.RetryAfter = -1
	case count+n <= rule.Limit:
		for j := int64(0); j < n; j++ {
			l.ts = append(l.ts, now)
		}
		count += n
		d.Allowed = true
	default:
		// The request fits once the (count+n-limit) oldest entries expire.
		need := count + n - rule.Limit
		d.RetryAfter = l.ts[need-1].Add(rule.Window).Sub(now)
	}
	l.expires = now.Add(rule.Window)
	d.Remaining = rule.Limit - count
	return d
}

// Sweep drops state that has fully expired. Call periodically (see RunSweeper).
// Returns the number of keys removed.
func (m *Memory) Sweep() int {
	now := m.clock.Now()
	removed := 0
	for i := range m.shards {
		s := &m.shards[i]
		s.mu.Lock()
		for k, b := range s.buckets {
			if now.After(b.expires) {
				delete(s.buckets, k)
				removed++
			}
		}
		for k, l := range s.logs {
			if now.After(l.expires) {
				delete(s.logs, k)
				removed++
			}
		}
		s.mu.Unlock()
	}
	return removed
}

// RunSweeper calls Sweep every interval until ctx is done.
func (m *Memory) RunSweeper(ctx context.Context, interval time.Duration) {
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			m.Sweep()
		}
	}
}
