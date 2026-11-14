package latency

import (
	"math"
	"math/rand"
	"sort"
	"testing"
)

func exact(values []float64, q float64) float64 {
	s := append([]float64(nil), values...)
	sort.Float64s(s)
	r := int(math.Ceil(q*float64(len(s)))) - 1
	return s[max(r, 0)]
}

func TestQuantilesAreWithinOnePercent(t *testing.T) {
	rng := rand.New(rand.NewSource(1))
	var values []float64
	h := Hist{}
	for i := 0; i < 20000; i++ {
		// Mostly quick, with a long slow tail: what a ping or an HTTP check
		// on a home network looks like.
		v := 2 + rng.ExpFloat64()*3
		if rng.Intn(20) == 0 {
			v = 200 + rng.Float64()*800
		}
		values = append(values, v)
		h.Add(v)
	}
	for _, q := range []float64{0.5, 0.9, 0.95, 0.99} {
		got, ok := h.Quantile(q)
		want := exact(values, q)
		if !ok || math.Abs(got-want)/want > 0.011 {
			t.Errorf("q%.2f: got %.3f, want %.3f", q, got, want)
		}
	}
}

func TestMergeIsExact(t *testing.T) {
	a, b, all := Hist{}, Hist{}, Hist{}
	for i := 1; i <= 500; i++ {
		v := float64(i) * 0.7
		if i%2 == 0 {
			a.Add(v)
		} else {
			b.Add(v)
		}
		all.Add(v)
	}
	a.Merge(b)
	if a.String() != all.String() {
		t.Fatal("merging two halves should give the same histogram as one")
	}
}

func TestEncodeRoundTrip(t *testing.T) {
	h := Hist{}
	for _, v := range []float64{0, 0.0005, 0.3, 1, 1, 12.5, 999, 60000} {
		h.Add(v)
	}
	h.Add(-1)
	h.Add(math.NaN())
	if h.Count() != 8 {
		t.Fatalf("count %d", h.Count())
	}
	back := Parse(h.String())
	if back.String() != h.String() || back.Count() != 8 {
		t.Fatalf("round trip: %q vs %q", back.String(), h.String())
	}
	if got := Parse("garbage,1:x,3:2"); got.Count() != 2 {
		t.Fatalf("damaged value: %v", got)
	}
	if (Hist{}).String() != "" || Parse("").Count() != 0 {
		t.Fatal("empty")
	}
	if _, ok := (Hist{}).Quantile(0.95); ok {
		t.Fatal("an empty histogram has no percentiles")
	}
	if p := h.Percentiles(); p.P50 == nil || p.P99 == nil || *p.P99 < *p.P50 {
		t.Fatalf("percentiles: %+v", p)
	}
}
