package actions

import (
	"net/http"
	"strconv"
	"strings"
	"time"
)

// maxRetryAfter bounds how long a Retry-After header can hold an alert back;
// a service asking for a day is treated as asking for an hour.
const maxRetryAfter = time.Hour

// retryAfter reads the wait a service asked for when it turned a request away
// for being too many (429) or for being briefly unavailable (503): either a
// number of seconds or an HTTP date. Anything else, or no header, is 0.
func retryAfter(resp *http.Response, now time.Time) time.Duration {
	if resp == nil || (resp.StatusCode != http.StatusTooManyRequests && resp.StatusCode != http.StatusServiceUnavailable) {
		return 0
	}
	v := strings.TrimSpace(resp.Header.Get("Retry-After"))
	if v == "" {
		return 0
	}
	var d time.Duration
	if secs, err := strconv.Atoi(v); err == nil {
		d = time.Duration(secs) * time.Second
	} else if t, err := http.ParseTime(v); err == nil {
		d = t.Sub(now)
	}
	return min(max(d, 0), maxRetryAfter)
}

func retryAfterNow(resp *http.Response) time.Duration { return retryAfter(resp, time.Now()) }
