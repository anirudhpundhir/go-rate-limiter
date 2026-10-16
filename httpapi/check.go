package httpapi

import (
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"

	"github.com/anirudhpundhir/go-rate-limiter/limiter"
)

// CheckRequest is the body of POST /v1/check.
type CheckRequest struct {
	Policy string `json:"policy"`
	Key    string `json:"key"`
	Cost   int64  `json:"cost,omitempty"` // default 1
}

// CheckResponse is returned with 200 whether or not the call is allowed;
// callers branch on Allowed. Errors use non-2xx statuses.
type CheckResponse struct {
	Allowed      bool   `json:"allowed"`
	Limit        int64  `json:"limit"`
	Remaining    int64  `json:"remaining"`
	RetryAfterMs int64  `json:"retry_after_ms"`
	Source       string `json:"source"`
}

type errorBody struct {
	Error string `json:"error"`
}

// Server serves the check API. Policies are configured server-side so that
// clients cannot choose their own limits.
type Server struct {
	Limiter  limiter.Limiter
	Policies map[string]limiter.Rule
	Logger   *slog.Logger
}

// Handler returns the API mux: POST /v1/check, GET /healthz.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/check", s.check)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNoContent) })
	return mux
}

func (s *Server) check(w http.ResponseWriter, r *http.Request) {
	var req CheckRequest
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 4096))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, errorBody{"invalid JSON: " + err.Error()})
		return
	}
	if req.Key == "" {
		writeJSON(w, http.StatusBadRequest, errorBody{"key is required"})
		return
	}
	if req.Cost == 0 {
		req.Cost = 1
	}
	rule, ok := s.Policies[req.Policy]
	if !ok {
		writeJSON(w, http.StatusNotFound, errorBody{"unknown policy"})
		return
	}
	d, err := s.Limiter.Allow(r.Context(), req.Policy+":"+req.Key, rule, req.Cost)
	switch {
	case errors.Is(err, limiter.ErrInvalidRule):
		writeJSON(w, http.StatusBadRequest, errorBody{err.Error()})
		return
	case err != nil:
		if s.Logger != nil {
			s.Logger.Error("check failed", "err", err)
		}
		writeJSON(w, http.StatusServiceUnavailable, errorBody{"limiter unavailable"})
		return
	}
	resp := CheckResponse{Allowed: d.Allowed, Limit: rule.Limit, Remaining: max(d.Remaining, 0), Source: d.Source, RetryAfterMs: d.RetryAfter.Milliseconds()}
	if d.RetryAfter < 0 {
		resp.RetryAfterMs = -1
	}
	writeJSON(w, http.StatusOK, resp)
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
