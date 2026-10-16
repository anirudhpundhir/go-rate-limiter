package limiter

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// Benchmarks measure per-decision latency/throughput. Memory benchmarks run
// anywhere; Redis ones need RATELIMIT_TEST_REDIS_ADDR. Keys: "hot" = all
// goroutines on one key (worst-case contention), "spread" = 10k keys.
func benchLimiter(b *testing.B, l Limiter, alg Algorithm, keys int) {
	rule := Rule{Algorithm: alg, Limit: 1 << 40, Window: time.Second} // never rejects
	if alg == SlidingWindow {
		rule = Rule{Algorithm: alg, Limit: 100, Window: time.Millisecond} // bounded log size
	}
	names := make([]string, keys)
	for i := range names {
		names[i] = fmt.Sprintf("k%d", i)
	}
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			if _, err := l.Allow(ctx, names[i%keys], rule, 1); err != nil {
				b.Error(err)
				return
			}
			i++
		}
	})
}

func BenchmarkMemory(b *testing.B) {
	for _, alg := range []Algorithm{TokenBucket, SlidingWindow} {
		for _, keys := range []int{1, 10000} {
			b.Run(fmt.Sprintf("%s/keys=%d", alg, keys), func(b *testing.B) { benchLimiter(b, NewMemory(nil), alg, keys) })
		}
	}
}

func BenchmarkRedis(b *testing.B) {
	cl := realRedis(b).(*redis.Client)
	for _, alg := range []Algorithm{TokenBucket, SlidingWindow} {
		for _, keys := range []int{1, 10000} {
			b.Run(fmt.Sprintf("%s/keys=%d", alg, keys), func(b *testing.B) {
				benchLimiter(b, NewRedis(cl, WithPrefix(uniquePrefix())), alg, keys)
			})
		}
	}
}
