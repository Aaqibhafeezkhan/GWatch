package store

import (
	"context"
	"math"
	"testing"
	"time"

	"github.com/jxburros/GWatch/internal/model"
)

// Percentiles survive every tier of rollups: one slow result in ten is
// visible at p95/p99 in the 5-minute, hourly and daily buckets, which an
// average hides; and a rebuild from raw results that retention has partly
// pruned leaves the bucket's histogram alone.
func TestRollupsKeepLatencyPercentiles(t *testing.T) {
	s := openTest(t)
	ctx := context.Background()
	n, err := s.CreateNode(ctx, model.Node{Name: "Web", Host: "web.lan", Enabled: true, Checks: []model.Check{{Name: "HTTP", Type: model.CheckHTTP, Enabled: true}}})
	if err != nil {
		t.Fatal(err)
	}
	id := n.Checks[0].ID
	start := time.Now().Add(-48 * time.Hour).Truncate(time.Hour)
	var results []model.Result
	for i := 0; i < 600; i++ { // one every 10 s for 100 minutes
		ms := 10.0
		if i%10 == 9 {
			ms = 900
		}
		results = append(results, model.Result{CheckID: id, Timestamp: start.Add(time.Duration(i) * 10 * time.Second), Success: true, Status: model.StatusUp, LatencyMS: &ms})
	}
	failed := model.Result{CheckID: id, Timestamp: start.Add(5 * time.Second), Status: model.StatusDown}
	results = append(results, failed)
	if err := s.InsertResultsBatch(ctx, results); err != nil {
		t.Fatal(err)
	}
	end := start.Add(2 * time.Hour)
	if _, err := s.RollupFromRaw(ctx, start, end); err != nil {
		t.Fatal(err)
	}
	if _, err := s.RollupUp(ctx, Bucket5m, Bucket1h, start, end); err != nil {
		t.Fatal(err)
	}
	if _, err := s.RollupUp(ctx, Bucket1h, Bucket1d, start, end); err != nil {
		t.Fatal(err)
	}
	near := func(p *float64, want float64) bool { return p != nil && math.Abs(*p-want)/want < 0.02 }
	for _, bucket := range []int{Bucket5m, Bucket1h, Bucket1d} {
		got, err := s.RollupsBetween(ctx, id, bucket, start.Add(-24*time.Hour), end.Add(24*time.Hour))
		if err != nil || len(got) == 0 {
			t.Fatalf("bucket %d: %v %d", bucket, err, len(got))
		}
		r := got[0]
		if !near(r.P50MS, 10) || !near(r.P99MS, 900) {
			t.Fatalf("bucket %d: p50 %v p99 %v (%q)", bucket, deref(r.P50MS), deref(r.P99MS), r.LatencyHist)
		}
		if !near(r.P95MS, 900) {
			t.Fatalf("5-minute p95 should catch the slow tenth: %v", deref(r.P95MS))
		}
	}
	// Retention prunes half of the first bucket's raw results; a rebuild
	// keeps the complete histogram rather than one from the fragment.
	first, _ := s.RollupsBetween(ctx, id, Bucket5m, start, start.Add(5*time.Minute))
	if _, err := s.exec(ctx, "DELETE FROM results WHERE check_id = ? AND ts < ?", id, start.Add(150*time.Second).UnixMilli()); err != nil {
		t.Fatal(err)
	}
	if _, err := s.RollupFromRaw(ctx, start, start.Add(5*time.Minute)); err != nil {
		t.Fatal(err)
	}
	again, _ := s.RollupsBetween(ctx, id, Bucket5m, start, start.Add(5*time.Minute))
	if again[0].LatencyHist != first[0].LatencyHist || again[0].Count != first[0].Count {
		t.Fatalf("a partial rebuild replaced the histogram: %q -> %q", first[0].LatencyHist, again[0].LatencyHist)
	}
	// The range summary reads percentiles from the buckets' histograms.
	series, err := s.History(ctx, n.Checks[0], n.Name, RangeSpec{"7d", 7 * 24 * time.Hour, Bucket5m}, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if !near(series.Summary.P50MS, 10) || !near(series.Summary.P99MS, 900) {
		t.Fatalf("summary percentiles: p50 %v p99 %v", deref(series.Summary.P50MS), deref(series.Summary.P99MS))
	}
}

func deref(p *float64) any {
	if p == nil {
		return nil
	}
	return *p
}
