package engine

import (
	"context"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/jxburros/GWatch/internal/model"
	"github.com/jxburros/GWatch/internal/store"
)

func TestFlapShare(t *testing.T) {
	steady := make([]bool, 21)
	if got := flapShare(steady); got != 0 {
		t.Fatalf("steady: %v", got)
	}
	every := make([]bool, 21)
	for i := range every {
		every[i] = i%2 == 0
	}
	if got := flapShare(every); math.Abs(got-1) > 1e-9 {
		t.Fatalf("changing every time: %v", got)
	}
	// One change at the oldest end weighs less than one at the newest.
	old, recent := make([]bool, 21), make([]bool, 21)
	old[0] = true
	recent[20] = true
	if flapShare(old) >= flapShare(recent) {
		t.Fatalf("newer changes should weigh more: old %v, recent %v", flapShare(old), flapShare(recent))
	}
}

// A check that goes down and comes back again and again alerts on its first
// outage as usual, then sends one flapping warning instead of an alert per
// change; once it settles and is down, it alerts again.
func TestFlappingCheckHoldsBackRepeatAlerts(t *testing.T) {
	e, st, _, mb := setup(t)
	n := regressionNode(t, e)
	c := n.Checks[0]
	e.settings.Alerts.CooldownMinutes = 0
	e.settings.Alerts.NotifyRecovery = false
	e.settings.Alerts.NotifyWarnings = true
	ctx := context.Background()
	at := time.Now()
	run := func(ok bool) {
		t.Helper()
		at = at.Add(time.Minute)
		r := model.Result{CheckID: c.ID, Timestamp: at, Success: ok, Status: model.StatusUp}
		if !ok {
			r.Status = model.StatusDown
		}
		if _, err := e.process(ctx, c, n, r, 0); err != nil {
			t.Fatal(err)
		}
		e.wg.Wait()
	}
	for i := 0; i < 6; i++ { // up, down, up, down ... twelve results
		run(true)
		run(false)
	}
	s, _ := e.State(c.ID)
	if !s.Flapping {
		t.Fatalf("a check changing every result should be flapping: share %.2f over %d", flapShare(s.FlapHistory), len(s.FlapHistory))
	}
	downs, warnings := 0, 0
	for _, subj := range mb.subjects() {
		switch {
		case strings.Contains(subj, "DOWN:"):
			downs++
		case strings.Contains(subj, "WARNING:"):
			warnings++
		}
	}
	if warnings != 1 {
		t.Fatalf("want one flapping warning, got %d: %v", warnings, mb.subjects())
	}
	before := downs
	run(true)
	run(false)
	run(true)
	run(false)
	if got := mb.count(); got != before+warnings {
		t.Fatalf("alerts sent while flapping: %v", mb.subjects())
	}
	// Settles down — and stays down long enough that the share drops.
	for i := 0; i < 20 && func() bool { s, _ := e.State(c.ID); return s.Flapping }(); i++ {
		run(false)
	}
	s, _ = e.State(c.ID)
	if s.Flapping || s.Status != model.StatusDown || !s.AlertActive {
		t.Fatalf("a check that settled while down should alert again: %+v", s)
	}
	evs, _ := st.ListEvents(ctx, store.EventFilter{Limit: 50, Types: []model.EventType{model.EventFlapping, model.EventFlappingEnded}})
	if len(evs) != 2 {
		t.Fatalf("want one flapping and one stopped-flapping event, got %d", len(evs))
	}
}

func TestFlapDetectionOff(t *testing.T) {
	st := &model.CheckState{Flapping: true, FlapHistory: []bool{true, false}}
	if _, ended := updateFlap(st, false); !ended || st.Flapping || st.FlapHistory != nil {
		t.Fatalf("switching it off should end flapping and forget the history: %+v", st)
	}
}
