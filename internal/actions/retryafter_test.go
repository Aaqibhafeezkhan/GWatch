package actions

import (
	"net/http"
	"testing"
	"time"
)

func TestRetryAfter(t *testing.T) {
	now := time.Date(2026, 11, 14, 12, 0, 0, 0, time.UTC)
	resp := func(code int, v string) *http.Response {
		h := http.Header{}
		if v != "" {
			h.Set("Retry-After", v)
		}
		return &http.Response{StatusCode: code, Header: h}
	}
	cases := []struct {
		r    *http.Response
		want time.Duration
	}{
		{resp(429, "30"), 30 * time.Second},
		{resp(503, now.Add(2*time.Minute).Format(http.TimeFormat)), 2 * time.Minute},
		{resp(429, "86400"), time.Hour},
		{resp(429, "soon"), 0},
		{resp(500, "30"), 0},
		{resp(429, ""), 0},
		{resp(429, "-5"), 0},
		{nil, 0},
	}
	for i, c := range cases {
		if got := retryAfter(c.r, now); got != c.want {
			t.Errorf("case %d: got %s, want %s", i, got, c.want)
		}
	}
}
