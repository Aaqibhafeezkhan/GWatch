package engine

import (
	"fmt"

	"github.com/jxburros/GWatch/internal/model"
)

// Flap detection (Settings › Alerts › flapping), after the method Nagios has
// used for decades: keep the last flapWindow judged states of a check, count
// how often it changed between down and not-down, weight the newer changes
// more than the older ones, and call the check flapping once that share
// passes flapStart. It stops flapping only when the share falls below the
// lower flapStop, so a check on the edge does not toggle in and out of it.
//
// The states counted are the check's judged status, after the failure
// threshold, not raw results: a single failed ping that the threshold
// absorbs is not a change anyone was told about, so it is not flapping
// either. The history is kept in memory only; after a restart it fills
// again within flapWindow results.
const (
	flapWindow     = 21   // states kept, so 20 possible changes
	flapMinHistory = 11   // states needed before a check can be called flapping
	flapStart      = 0.30 // weighted share of changes that begins flapping
	flapStop       = 0.15 // and that ends it
)

// flapShare is the weighted share of changes in a history of down (true) /
// not-down (false) states, oldest first: 0 when it never changed, 1 when it
// changed every time. The newest change counts 50% more than the oldest, as
// in Nagios, so a check that has settled down stops flapping sooner.
func flapShare(history []bool) float64 {
	changes := len(history) - 1
	if changes < 1 {
		return 0
	}
	var sum, total float64
	for i := 1; i <= changes; i++ {
		w := 0.8
		if changes > 1 {
			w += 0.4 * float64(i-1) / float64(changes-1)
		}
		total += w
		if history[i] != history[i-1] {
			sum += w
		}
	}
	return sum / total
}

// updateFlap adds the check's newly judged state to its history and reports
// whether it began or stopped flapping with this result.
func updateFlap(st *model.CheckState, enabled bool) (began, ended bool) {
	if !enabled {
		ended = st.Flapping
		st.Flapping, st.FlapHistory = false, nil
		return false, ended
	}
	h := append(st.FlapHistory, st.Status == model.StatusDown)
	if len(h) > flapWindow {
		h = h[len(h)-flapWindow:]
	}
	// A fresh slice each time: the staged state is a shallow copy of the
	// live one, and appending in place would change the live history before
	// the result is committed.
	st.FlapHistory = append([]bool(nil), h...)
	share := flapShare(st.FlapHistory)
	switch {
	case !st.Flapping && len(st.FlapHistory) >= flapMinHistory && share >= flapStart:
		st.Flapping = true
		return true, false
	case st.Flapping && share < flapStop:
		st.Flapping = false
		return false, true
	}
	return false, false
}

// flapChanges counts the changes in a history, for the timeline.
func flapChanges(history []bool) int {
	n := 0
	for i := 1; i < len(history); i++ {
		if history[i] != history[i-1] {
			n++
		}
	}
	return n
}

func flapDetail(st *model.CheckState) string {
	return fmt.Sprintf("Went down or came back %d times in its last %d results. Further down and recovery alerts are held back until it settles; results are still recorded.", flapChanges(st.FlapHistory), len(st.FlapHistory))
}
