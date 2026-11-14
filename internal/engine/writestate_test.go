package engine

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jxburros/GWatch/internal/model"
)

func TestWriteBackoffGrowsToFiveMinutes(t *testing.T) {
	want := []time.Duration{5 * time.Second, 10 * time.Second, 20 * time.Second, 40 * time.Second, 80 * time.Second, 160 * time.Second, 5 * time.Minute, 5 * time.Minute}
	for i, w := range want {
		if got := writeBackoff(i + 1); got != w {
			t.Fatalf("failure %d: wait %s, want %s", i+1, got, w)
		}
	}
}

// #211: a result that cannot be written leaves the alerting state alone, as
// before, but the check is no longer due again on the very next tick, and
// Monitor health says that results are not being kept.
func TestFailedWriteBacksOffAndShowsInHealth(t *testing.T) {
	e, _, _, _ := setup(t)
	n := regressionNode(t, e)
	c := n.Checks[0]
	save := e.recordResult
	e.recordResult = func(context.Context, model.Result, model.CheckState) (model.Result, error) {
		return model.Result{}, errors.New("disk full")
	}
	start := time.Now()
	for i, want := range []time.Duration{5 * time.Second, 10 * time.Second, 20 * time.Second} {
		if _, err := e.process(context.Background(), c, n, model.Result{CheckID: c.ID, Timestamp: time.Now(), Status: model.StatusDown}, 0); err == nil {
			t.Fatal("expected the write to fail")
		}
		st, _ := e.State(c.ID)
		if st.ConsecutiveFailures != 0 || st.Status == model.StatusDown {
			t.Fatalf("attempt %d: alerting state advanced on a failed write: %+v", i+1, st)
		}
		if st.NextRunAt == nil || st.NextRunAt.Before(start.Add(want)) || st.NextRunAt.After(time.Now().Add(want+time.Second)) {
			t.Fatalf("attempt %d: next run %v, want about %s from now", i+1, st.NextRunAt, want)
		}
	}
	h := e.Health(context.Background())
	if h.ResultWriteFailures != 1 || h.ResultWriteError != "disk full" || h.ResultWriteFailingSince == nil {
		t.Fatalf("health does not report the failing writes: %d %q %v", h.ResultWriteFailures, h.ResultWriteError, h.ResultWriteFailingSince)
	}
	e.recordResult = save
	if _, err := e.process(context.Background(), c, n, model.Result{CheckID: c.ID, Timestamp: time.Now(), Status: model.StatusDown}, 0); err != nil {
		t.Fatal(err)
	}
	if h := e.Health(context.Background()); h.ResultWriteFailures != 0 || h.ResultWriteError != "" || h.ResultWriteFailingSince != nil {
		t.Fatalf("health still warns after a write succeeded: %+v", h)
	}
	if st, _ := e.State(c.ID); st.NextRunAt == nil || st.NextRunAt.Before(time.Now().Add(50*time.Second)) {
		t.Fatalf("a successful write should schedule the ordinary interval: %v", st.NextRunAt)
	}
}

// #210: while one result is being written, the engine lock is free, so the
// overview, health and other checks are not held up behind a slow disk.
func TestSlowWriteDoesNotHoldTheEngineLock(t *testing.T) {
	e, _, _, _ := setup(t)
	n := regressionNode(t, e)
	c := n.Checks[0]
	save := e.recordResult
	entered, release := make(chan struct{}), make(chan struct{})
	e.recordResult = func(ctx context.Context, r model.Result, st model.CheckState) (model.Result, error) {
		close(entered)
		<-release
		return save(ctx, r, st)
	}
	done := make(chan error, 1)
	go func() {
		_, err := e.process(context.Background(), c, n, model.Result{CheckID: c.ID, Timestamp: time.Now(), Success: true, Status: model.StatusUp}, 0)
		done <- err
	}()
	<-entered
	locked := make(chan struct{})
	go func() {
		e.mu.Lock()
		e.mu.Unlock() //nolint:staticcheck // only proving the lock can be taken
		close(locked)
	}()
	select {
	case <-locked:
	case <-time.After(2 * time.Second):
		close(release)
		t.Fatal("the engine lock is held while the result is written")
	}
	if st, _ := e.State(c.ID); st.Status == model.StatusUp {
		t.Fatal("the state was committed before the write finished")
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if st, _ := e.State(c.ID); st.Status != model.StatusUp {
		t.Fatalf("the state was not committed after the write: %+v", st)
	}
}

// A silence set while a result is being written is not undone when the
// result's state is committed, in memory or in the database.
func TestSilenceDuringWriteIsKept(t *testing.T) {
	e, st, _, _ := setup(t)
	n := regressionNode(t, e)
	c := n.Checks[0]
	save := e.recordResult
	entered, release := make(chan struct{}), make(chan struct{})
	e.recordResult = func(ctx context.Context, r model.Result, s model.CheckState) (model.Result, error) {
		close(entered)
		<-release
		return save(ctx, r, s)
	}
	done := make(chan error, 1)
	go func() {
		_, err := e.process(context.Background(), c, n, model.Result{CheckID: c.ID, Timestamp: time.Now(), Status: model.StatusDown}, 0)
		done <- err
	}()
	<-entered
	if _, err := e.Silence(context.Background(), c.ID, time.Hour); err != nil {
		t.Fatal(err)
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	e.wg.Wait()
	mem, _ := e.State(c.ID)
	if mem.SilencedUntil == nil || mem.Status != model.StatusDown || mem.ConsecutiveFailures != 1 {
		t.Fatalf("merged state lost a change: %+v", mem)
	}
	states, err := st.ListStates(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if got := states[c.ID]; got.SilencedUntil == nil || got.Status != model.StatusDown {
		t.Fatalf("stored state lost a change: %+v", got)
	}
}

func TestMergeStateKeepsTheLaterChange(t *testing.T) {
	before := model.CheckState{Status: model.StatusUp, ConsecutiveFailures: 0, LastMessage: "ok"}
	ours := before
	ours.Status, ours.ConsecutiveFailures, ours.LastMessage = model.StatusDown, 1, "timed out"
	theirs := before
	theirs.Status = model.StatusPaused
	until := time.Now()
	theirs.SilencedUntil = &until
	got := mergeState(before, ours, theirs)
	if got.Status != model.StatusPaused || got.SilencedUntil == nil || got.ConsecutiveFailures != 1 || got.LastMessage != "timed out" {
		t.Fatalf("merge: %+v", got)
	}
}
