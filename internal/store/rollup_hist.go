package store

import (
	"context"
	"database/sql"
	"time"

	"github.com/jxburros/GWatch/internal/latency"
	"github.com/jxburros/GWatch/internal/model"
)

// Latency histograms for rollup buckets (see internal/latency). The rollups
// themselves are computed in SQL, which has no portable way to build a
// histogram, so these passes run after each rollup statement: one builds the
// 5-minute buckets' histograms from raw results, the other merges smaller
// buckets' histograms into larger ones.
//
// A bucket's histogram is written only when everything its row was rolled up
// from was there to build it from: the same rule the rollups follow (a
// rebuild from a partly pruned source never replaces a complete aggregate).
// A bucket rolled up before histograms existed, or from such buckets, keeps
// an empty histogram and shows no percentiles; it is not guessed at.

type histKey struct {
	check int64
	start int64
}

// histFromRaw builds the histograms of the 5-minute buckets starting in
// [fromB, toB) (unix seconds) from raw results.
func (s *Store) histFromRaw(ctx context.Context, fromB, toB int64) error {
	rows, err := s.query(ctx, `SELECT check_id, ts, latency_ms FROM results WHERE ts >= ? AND ts < ? AND success = 1 AND latency_ms IS NOT NULL ORDER BY check_id, ts`, fromB*1000, toB*1000)
	if err != nil {
		return err
	}
	hists := map[histKey]latency.Hist{}
	for rows.Next() {
		var check, ts int64
		var ms float64
		if err := rows.Scan(&check, &ts, &ms); err != nil {
			rows.Close()
			return err
		}
		sec := ts / 1000
		k := histKey{check, sec - sec%Bucket5m}
		if hists[k] == nil {
			hists[k] = latency.Hist{}
		}
		hists[k].Add(ms)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}
	if len(hists) == 0 {
		return nil
	}
	// The bucket's latencies are complete when it holds as many successful
	// results with a latency as raw results still have.
	counts, err := s.successLatencyCounts(ctx, fromB, toB)
	if err != nil {
		return err
	}
	return s.writeHists(ctx, Bucket5m, hists, counts)
}

// successLatencyCounts counts, per 5-minute bucket, the successful results
// with a latency that the bucket's rollup row was built from. A rollup row
// keeps its complete aggregate when raw results have since been pruned, and
// then this is larger than what raw results can still give.
func (s *Store) successLatencyCounts(ctx context.Context, fromB, toB int64) (map[histKey]uint64, error) {
	rows, err := s.query(ctx, `SELECT check_id, bucket_start, count, success_count FROM rollups WHERE bucket_seconds = ? AND bucket_start >= ? AND bucket_start < ?`, Bucket5m, fromB, toB)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	type row struct{ count, success int64 }
	stored := map[histKey]row{}
	for rows.Next() {
		var k histKey
		var r row
		if err := rows.Scan(&k.check, &k.start, &r.count, &r.success); err != nil {
			return nil, err
		}
		stored[k] = r
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	raw, err := s.query(ctx, `SELECT check_id, ts, success, CASE WHEN latency_ms IS NULL THEN 0 ELSE 1 END FROM results WHERE ts >= ? AND ts < ?`, fromB*1000, toB*1000)
	if err != nil {
		return nil, err
	}
	defer raw.Close()
	have := map[histKey]int64{}
	withLatency := map[histKey]uint64{}
	for raw.Next() {
		var check, ts int64
		var success, hasLatency int
		if err := raw.Scan(&check, &ts, &success, &hasLatency); err != nil {
			return nil, err
		}
		sec := ts / 1000
		k := histKey{check, sec - sec%Bucket5m}
		have[k]++
		if success == 1 && hasLatency == 1 {
			withLatency[k]++
		}
	}
	if err := raw.Err(); err != nil {
		return nil, err
	}
	out := map[histKey]uint64{}
	for k, r := range stored {
		if have[k] == r.count { // every result the row counts is still here
			out[k] = withLatency[k]
		}
	}
	return out, nil
}

// histUp merges the histograms of the srcBucket buckets in [fromB, toB) into
// the dstBucket buckets they roll up into.
func (s *Store) histUp(ctx context.Context, srcBucket, dstBucket int, fromB, toB int64) error {
	rows, err := s.query(ctx, `SELECT check_id, bucket_start, count, success_count, latency_hist FROM rollups WHERE bucket_seconds = ? AND bucket_start >= ? AND bucket_start < ?`, srcBucket, fromB, toB)
	if err != nil {
		return err
	}
	hists := map[histKey]latency.Hist{}
	srcCount := map[histKey]int64{}
	incomplete := map[histKey]bool{}
	for rows.Next() {
		var check, start, count, success int64
		var raw string
		if err := rows.Scan(&check, &start, &count, &success, &raw); err != nil {
			rows.Close()
			return err
		}
		k := histKey{check, s.parentBucket(start, dstBucket)}
		srcCount[k] += count
		if raw == "" {
			if success > 0 {
				incomplete[k] = true // rolled up before histograms, or from a partial source
			}
			continue
		}
		if hists[k] == nil {
			hists[k] = latency.Hist{}
		}
		hists[k].Merge(latency.Parse(raw))
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}
	// The larger bucket is complete when it counts exactly what its smaller
	// buckets do, and each of them had a histogram.
	dst, err := s.query(ctx, `SELECT check_id, bucket_start, count FROM rollups WHERE bucket_seconds = ? AND bucket_start >= ? AND bucket_start < ?`, dstBucket, s.parentBucket(fromB, dstBucket), toB)
	if err != nil {
		return err
	}
	complete := map[histKey]uint64{}
	for dst.Next() {
		var k histKey
		var count int64
		if err := dst.Scan(&k.check, &k.start, &count); err != nil {
			dst.Close()
			return err
		}
		if h := hists[k]; h != nil && !incomplete[k] && srcCount[k] == count {
			complete[k] = h.Count()
		}
	}
	dst.Close()
	if err := dst.Err(); err != nil {
		return err
	}
	return s.writeHists(ctx, dstBucket, hists, complete)
}

// parentBucket is the start of the dstBucket bucket a bucket starting at
// start belongs to: a local calendar day for daily buckets (see RollupUp).
func (s *Store) parentBucket(start int64, dstBucket int) int64 {
	if dstBucket == Bucket1d {
		return localDay(time.Unix(start, 0)).Unix()
	}
	return start - start%int64(dstBucket)
}

// writeHists stores each histogram whose bucket is complete: want holds how
// many latencies a complete histogram for the bucket has.
func (s *Store) writeHists(ctx context.Context, bucket int, hists map[histKey]latency.Hist, want map[histKey]uint64) error {
	return s.writeTx(ctx, func(tx *wtx) error {
		var stmt *sql.Stmt
		for k, h := range hists {
			n, ok := want[k]
			if !ok || h.Count() != n {
				continue
			}
			if stmt == nil {
				var err error
				if stmt, err = tx.prepare(ctx, `UPDATE rollups SET latency_hist = ? WHERE check_id = ? AND bucket_seconds = ? AND bucket_start = ?`); err != nil {
					return err
				}
				defer stmt.Close()
			}
			if _, err := stmt.ExecContext(ctx, h.String(), k.check, bucket, k.start); err != nil {
				return err
			}
		}
		return nil
	})
}

// applyPercentiles sets a summary's percentiles from the histogram of the
// whole range.
func applyPercentiles(sum *model.HistorySummary, h latency.Hist) {
	p := h.Percentiles()
	sum.P50MS, sum.P95MS, sum.P99MS = p.P50, p.P95, p.P99
}
