// Package latency keeps latency distributions small enough to store with
// every rollup bucket and exact enough to read percentiles from.
//
// A rollup used to keep only the minimum, maximum and average latency of its
// bucket, so a month or a year of history could say what a check usually
// took but not how slow it was at its worst ordinary moments: an average
// hides the one request in twenty that takes a second. Percentiles cannot be
// combined (the 95th percentile of a day is not the average of its hours'),
// but histograms can, so each bucket keeps one and the percentiles are read
// from it.
//
// The histogram is the one DDSketch uses: logarithmic bins, each about 2%
// wider than the one before, so any percentile read from it is within about
// 1% of the true value whatever the latency's scale, and two histograms
// merge exactly by adding their counts. Only bins that were hit are stored.
// The code is a few dozen lines, which is why it is written here rather than
// taken as a dependency.
package latency

import (
	"math"
	"sort"
	"strconv"
	"strings"
)

// gamma is the ratio between neighbouring bins: a relative accuracy of
// (gamma-1)/(gamma+1), about 1%.
const gamma = 1.02

var logGamma = math.Log(gamma)

// minValue is the smallest latency kept apart from zero, in milliseconds;
// anything at or below it (including zero) shares the lowest bin.
const minValue = 0.001

// Hist is a latency histogram in milliseconds: bin index to count.
type Hist map[int]uint64

func bin(ms float64) int {
	if ms <= minValue {
		ms = minValue
	}
	return int(math.Ceil(math.Log(ms) / logGamma))
}

// value is the latency a bin stands for: the midpoint that keeps the
// relative error of every value in it within the stated accuracy.
func value(i int) float64 {
	return 2 * math.Pow(gamma, float64(i)) / (gamma + 1)
}

// Add records one latency. Negative, NaN and infinite values are ignored.
func (h Hist) Add(ms float64) {
	if math.IsNaN(ms) || math.IsInf(ms, 0) || ms < 0 {
		return
	}
	h[bin(ms)]++
}

// Merge adds another histogram's counts into h.
func (h Hist) Merge(o Hist) {
	for i, n := range o {
		h[i] += n
	}
}

// Count is how many latencies the histogram holds.
func (h Hist) Count() uint64 {
	var n uint64
	for _, c := range h {
		n += c
	}
	return n
}

// Quantile returns the latency below which q of the recorded latencies fall
// (q between 0 and 1), and false for an empty histogram.
func (h Hist) Quantile(q float64) (float64, bool) {
	total := h.Count()
	if total == 0 {
		return 0, false
	}
	q = min(max(q, 0), 1)
	rank := uint64(math.Ceil(q * float64(total)))
	if rank == 0 {
		rank = 1
	}
	keys := make([]int, 0, len(h))
	for i := range h {
		keys = append(keys, i)
	}
	sort.Ints(keys)
	var seen uint64
	for _, i := range keys {
		seen += h[i]
		if seen >= rank {
			return value(i), true
		}
	}
	return value(keys[len(keys)-1]), true
}

// String encodes the histogram for storage: "bin:count" pairs, comma
// separated, in bin order. An empty histogram is "".
func (h Hist) String() string {
	if len(h) == 0 {
		return ""
	}
	keys := make([]int, 0, len(h))
	for i, n := range h {
		if n > 0 {
			keys = append(keys, i)
		}
	}
	sort.Ints(keys)
	var b strings.Builder
	for k, i := range keys {
		if k > 0 {
			b.WriteByte(',')
		}
		b.WriteString(strconv.Itoa(i))
		b.WriteByte(':')
		b.WriteString(strconv.FormatUint(h[i], 10))
	}
	return b.String()
}

// Parse reads a histogram String wrote. Anything it cannot read is skipped,
// so a damaged value reads as fewer samples rather than an error.
func Parse(s string) Hist {
	h := Hist{}
	for _, part := range strings.Split(s, ",") {
		k, v, ok := strings.Cut(part, ":")
		if !ok {
			continue
		}
		i, err1 := strconv.Atoi(k)
		n, err2 := strconv.ParseUint(v, 10, 64)
		if err1 != nil || err2 != nil {
			continue
		}
		h[i] += n
	}
	return h
}

// Percentiles are the three a chart shows.
type Percentiles struct {
	P50, P95, P99 *float64
}

// Percentiles reads the median, 95th and 99th percentiles, nil when empty.
func (h Hist) Percentiles() Percentiles {
	var p Percentiles
	for _, x := range []struct {
		q   float64
		dst **float64
	}{{0.50, &p.P50}, {0.95, &p.P95}, {0.99, &p.P99}} {
		if v, ok := h.Quantile(x.q); ok {
			v := v
			*x.dst = &v
		}
	}
	return p
}
