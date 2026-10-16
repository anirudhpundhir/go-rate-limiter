package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/anirudhpundhir/go-rate-limiter/limiter"
)

func TestParsePolicy(t *testing.T) {
	name, r, err := ParsePolicy("api=token_bucket:100:1m")
	if err != nil || name != "api" || r.Limit != 100 || r.Window != time.Minute || r.Algorithm != limiter.TokenBucket {
		t.Fatalf("got %s %+v %v", name, r, err)
	}
	for _, bad := range []string{"api", "=token_bucket:1:1s", "a=token_bucket:x:1s", "a=nope:1:1s", "a=sliding_window:1:zz", "a=sliding_window:0:1s"} {
		if _, _, err := ParsePolicy(bad); err == nil {
			t.Errorf("%q accepted", bad)
		}
	}
}

func TestMiddleware(t *testing.T) {
	ok := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(200) })
	h := Middleware(MiddlewareConfig{
		Limiter: limiter.NewMemory(nil),
		Rule:    limiter.Rule{Algorithm: limiter.SlidingWindow, Limit: 2, Window: time.Minute},
		Key:     ByHeader("X-Api-Key"),
	})(ok)
	do := func(key string) *httptest.ResponseRecorder {
		req := httptest.NewRequest("GET", "/", nil)
		if key != "" {
			req.Header.Set("X-Api-Key", key)
		}
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		return rec
	}
	if rec := do("a"); rec.Code != 200 || rec.Header().Get("RateLimit-Remaining") != "1" {
		t.Fatalf("1st: %d %v", rec.Code, rec.Header())
	}
	do("a")
	rec := do("a")
	if rec.Code != http.StatusTooManyRequests || rec.Header().Get("Retry-After") != "60" {
		t.Fatalf("3rd: %d %v", rec.Code, rec.Header())
	}
	if rec := do("b"); rec.Code != 200 {
		t.Fatal("other key limited")
	}
	for i := 0; i < 5; i++ {
		if rec := do(""); rec.Code != 200 {
			t.Fatal("unkeyed request limited")
		}
	}
}

type broken struct{}

func (broken) Allow(context.Context, string, limiter.Rule, int64) (limiter.Decision, error) {
	return limiter.Decision{}, errors.New("down")
}

func TestMiddlewareFailOpenClosed(t *testing.T) {
	ok := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(200) })
	rule := limiter.Rule{Algorithm: limiter.TokenBucket, Limit: 1, Window: time.Second}
	for _, tc := range []struct {
		closed bool
		want   int
	}{{false, 200}, {true, 503}} {
		h := Middleware(MiddlewareConfig{Limiter: broken{}, Rule: rule, FailClosed: tc.closed})(ok)
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest("GET", "/", nil))
		if rec.Code != tc.want {
			t.Fatalf("closed=%v: %d", tc.closed, rec.Code)
		}
	}
}

func TestCheckAPI(t *testing.T) {
	s := &Server{Limiter: limiter.NewMemory(nil), Policies: map[string]limiter.Rule{
		"login": {Algorithm: limiter.SlidingWindow, Limit: 3, Window: time.Minute},
	}}
	srv := httptest.NewServer(s.Handler())
	defer srv.Close()
	post := func(body string) (int, CheckResponse) {
		resp, err := http.Post(srv.URL+"/v1/check", "application/json", strings.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		var cr CheckResponse
		_ = json.NewDecoder(resp.Body).Decode(&cr)
		return resp.StatusCode, cr
	}
	code, cr := post(`{"policy":"login","key":"u1","cost":2}`)
	if code != 200 || !cr.Allowed || cr.Remaining != 1 || cr.Limit != 3 {
		t.Fatalf("%d %+v", code, cr)
	}
	code, cr = post(`{"policy":"login","key":"u1","cost":2}`)
	if code != 200 || cr.Allowed || cr.RetryAfterMs < 59000 || cr.RetryAfterMs > 60000 {
		t.Fatalf("%d %+v", code, cr)
	}
	if code, cr = post(`{"policy":"login","key":"u1","cost":9}`); cr.RetryAfterMs != -1 {
		t.Fatalf("cost>limit: %d %+v", code, cr)
	}
	for body, want := range map[string]int{
		`{"policy":"nope","key":"u"}`:            404,
		`{"policy":"login"}`:                     400,
		`{"policy":"login","key":"u","x":1}`:     400,
		`{"policy":"login","key":"u","cost":-1}`: 400,
		`not json`:                               400,
	} {
		if code, _ := post(body); code != want {
			t.Errorf("%s: %d want %d", body, code, want)
		}
	}
	s.Limiter = broken{}
	if code, _ := post(`{"policy":"login","key":"u"}`); code != 503 {
		t.Errorf("broken limiter: %d", code)
	}
}
