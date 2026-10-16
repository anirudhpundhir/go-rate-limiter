// Command ratelimiterd serves the rate-limit check API.
//
//	ratelimiterd -addr :8080 -redis localhost:6379 \
//	  -policy api=token_bucket:100:1m -policy login=sliding_window:5:15m
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/anirudhpundhir/go-rate-limiter/httpapi"
	"github.com/anirudhpundhir/go-rate-limiter/limiter"
	"github.com/redis/go-redis/v9"
)

type policyFlags map[string]limiter.Rule

func (p policyFlags) String() string { return fmt.Sprint(map[string]limiter.Rule(p)) }
func (p policyFlags) Set(s string) error {
	name, r, err := httpapi.ParsePolicy(s)
	if err != nil {
		return err
	}
	p[name] = r
	return nil
}

func main() {
	policies := policyFlags{}
	addr := flag.String("addr", ":8080", "listen address")
	redisAddr := flag.String("redis", "", "Redis address; empty = in-memory only")
	localScale := flag.Float64("local-scale", 1, "multiplier for limits while degraded to local fallback (e.g. 1/replicas)")
	redisTimeout := flag.Duration("redis-timeout", 50*time.Millisecond, "per-command Redis timeout")
	flag.Var(policies, "policy", "name=algorithm:limit:window (repeatable)")
	flag.Parse()
	log := slog.New(slog.NewJSONHandler(os.Stderr, nil))
	if len(policies) == 0 {
		log.Error("at least one -policy is required")
		os.Exit(2)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	mem := limiter.NewMemory(nil)
	go mem.RunSweeper(ctx, time.Minute)
	var l limiter.Limiter = mem
	if *redisAddr != "" {
		rc := redis.NewClient(&redis.Options{Addr: *redisAddr, ReadTimeout: *redisTimeout, WriteTimeout: *redisTimeout, DialTimeout: *redisTimeout})
		defer rc.Close()
		fb := limiter.NewFallback(limiter.NewRedis(rc), mem)
		fb.LocalScale = *localScale
		fb.OnFallback = func(err error) { log.Warn("redis unavailable, using local fallback", "err", err) }
		l = fb
	}

	srv := &http.Server{
		Addr:              *addr,
		Handler:           (&httpapi.Server{Limiter: l, Policies: policies, Logger: log}).Handler(),
		ReadHeaderTimeout: 5 * time.Second,
	}
	go func() {
		<-ctx.Done()
		sctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = srv.Shutdown(sctx)
	}()
	log.Info("listening", "addr", *addr, "redis", *redisAddr, "policies", len(policies))
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Error("server", "err", err)
		os.Exit(1)
	}
}
