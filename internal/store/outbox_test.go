package store

import (
	"context"
	"testing"
	"time"

	"github.com/jxburros/GWatch/internal/model"
)

func TestAlertOutbox(t *testing.T) {
	s := openTest(t)
	ctx := context.Background()
	n, err := s.CreateNode(ctx, model.Node{Name: "Router", Host: "192.168.1.1", Enabled: true, Checks: []model.Check{{Name: "Ping", Type: model.CheckPing, Enabled: true, IntervalSeconds: 60}}})
	if err != nil {
		t.Fatal(err)
	}
	cid := n.Checks[0].ID
	raised := time.Now().Truncate(time.Millisecond)
	first, err := s.QueueAlert(ctx, QueuedAlert{CheckID: cid, Target: "email", Kind: "down", CreatedAt: raised, NextAt: raised.Add(time.Minute), Attempts: 1, LastError: "connection refused", Payload: `{"kind":"down"}`})
	if err != nil {
		t.Fatal(err)
	}
	// A remote collector's alert has no check row; it queues all the same.
	if _, err := s.QueueAlert(ctx, QueuedAlert{CheckID: -7, Target: "channel:abc", Kind: "recovered", CreatedAt: raised, NextAt: raised, Payload: `{}`}); err != nil {
		t.Fatal(err)
	}
	q, err := s.QueuedAlerts(ctx)
	if err != nil || len(q) != 2 {
		t.Fatalf("queued: %v %+v", err, q)
	}
	if q[0].ID != first || q[0].Target != "email" || q[0].Attempts != 1 || q[0].LastError != "connection refused" || !q[0].CreatedAt.Equal(raised) || !q[0].NextAt.Equal(raised.Add(time.Minute)) {
		t.Fatalf("first row: %+v", q[0])
	}
	if err := s.RescheduleQueuedAlert(ctx, first, 2, raised.Add(3*time.Minute), "timed out"); err != nil {
		t.Fatal(err)
	}
	q, _ = s.QueuedAlerts(ctx)
	if q[0].Attempts != 2 || q[0].LastError != "timed out" || !q[0].NextAt.Equal(raised.Add(3*time.Minute)) {
		t.Fatalf("rescheduled: %+v", q[0])
	}
	if err := s.DeleteQueuedAlert(ctx, first); err != nil {
		t.Fatal(err)
	}
	if q, _ = s.QueuedAlerts(ctx); len(q) != 1 || q[0].CheckID != -7 {
		t.Fatalf("after delete: %+v", q)
	}
	// A restore replaces the configuration and what was waiting for it.
	if err := s.RestoreNodes(ctx, nil, false); err != nil {
		t.Fatal(err)
	}
	if q, _ = s.QueuedAlerts(ctx); len(q) != 0 {
		t.Fatalf("a restore should empty the outbox: %+v", q)
	}
}
