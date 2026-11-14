package engine

import (
	"context"
	"testing"
	"time"

	"github.com/jxburros/GWatch/internal/model"
)

func TestRecheckAfter(t *testing.T) {
	s := model.DefaultSettings()
	s.Alerts.FailureThreshold = 3
	c := model.Check{IntervalSeconds: 300}
	if got := recheckAfter(c, s); got != 30*time.Second {
		t.Fatalf("default re-check: %s", got)
	}
	c.IntervalSeconds = 20
	if got := recheckAfter(c, s); got != 0 {
		t.Fatalf("an interval shorter than the re-check should be left alone: %s", got)
	}
	c.IntervalSeconds, c.FailureThreshold = 300, 1
	if got := recheckAfter(c, s); got != 0 {
		t.Fatalf("one failure is already down, so there is nothing to confirm: %s", got)
	}
	c.FailureThreshold = 0
	s.Alerts.RecheckSeconds = 0
	if got := recheckAfter(c, s); got != 0 {
		t.Fatalf("re-check switched off: %s", got)
	}
}

// A five-minute check with a threshold of three is confirmed every 30 s once
// it starts failing, then goes back to its interval when it is down or passes.
func TestFailingCheckIsConfirmedSooner(t *testing.T) {
	e, st, _, _ := setup(t)
	ctx := context.Background()
	n, err := st.CreateNode(ctx, model.Node{Name: "NAS", Host: "nas.local", Enabled: true, Checks: []model.Check{{Name: "Ping", Type: model.CheckPing, Enabled: true, IntervalSeconds: 300, TimeoutSeconds: 1, FailureThreshold: 3}}})
	if err != nil {
		t.Fatal(err)
	}
	if err := e.ReloadConfig(ctx); err != nil {
		t.Fatal(err)
	}
	e.ctx = ctx
	t.Cleanup(e.wg.Wait)
	c := n.Checks[0]
	next := func() time.Duration {
		s, _ := e.State(c.ID)
		return time.Until(*s.NextRunAt).Round(10 * time.Second)
	}
	run := func(ok bool) {
		t.Helper()
		r := model.Result{CheckID: c.ID, Timestamp: time.Now(), Success: ok, Status: model.StatusUp}
		if !ok {
			r.Status = model.StatusDown
		}
		if _, err := e.process(ctx, c, n, r, 0); err != nil {
			t.Fatal(err)
		}
	}
	run(false)
	if got := next(); got != 30*time.Second {
		t.Fatalf("first failure: next run in %s, want 30s", got)
	}
	run(true)
	if got := next(); got != 300*time.Second {
		t.Fatalf("a pass goes back to the interval: %s", got)
	}
	run(false)
	run(false)
	if got := next(); got != 30*time.Second {
		t.Fatalf("second failure: next run in %s, want 30s", got)
	}
	run(false)
	if s, _ := e.State(c.ID); s.Status != model.StatusDown {
		t.Fatalf("three failures should be down: %+v", s)
	}
	if got := next(); got != 300*time.Second {
		t.Fatalf("a check that is down waits its interval: %s", got)
	}
}
