package engine

import (
	"reflect"
	"time"

	"github.com/jxburros/GWatch/internal/model"
)

// Backing off after a failed result write (#211). The first retry comes
// quickly, in case the database was only briefly locked; after that the wait
// doubles to at most five minutes, so a full disk or an unreachable database
// server costs a handful of attempts an hour per check rather than one a
// second.
const (
	writeRetryFirst = 5 * time.Second
	writeRetryMost  = 5 * time.Minute
)

// writeBackoff is how long a check waits after its n-th consecutive failed
// write (n >= 1).
func writeBackoff(n int) time.Duration {
	d := writeRetryFirst
	for i := 1; i < n && d < writeRetryMost; i++ {
		d *= 2
	}
	return min(d, writeRetryMost)
}

// noteWriteFailureLocked records a failed result write for a check and
// returns how long the check should wait before it runs again. The caller
// holds e.mu.
func (e *Engine) noteWriteFailureLocked(checkID int64, err error, now time.Time) time.Duration {
	e.writeFails[checkID]++
	e.writeErr = err.Error()
	if e.writeFailSince == nil {
		e.writeFailSince = ptrTime(now)
	}
	return writeBackoff(e.writeFails[checkID])
}

// noteWriteSuccessLocked clears a check's run of failed writes, and the
// Monitor health warning once no check is failing any more. The caller holds
// e.mu.
func (e *Engine) noteWriteSuccessLocked(checkID int64) {
	if _, failing := e.writeFails[checkID]; !failing {
		return
	}
	delete(e.writeFails, checkID)
	if len(e.writeFails) == 0 {
		e.writeErr = ""
		e.writeFailSince = nil
	}
}

// mergeState lays the changes a result made (from before to ours) over a
// state that something else changed in the meantime (theirs). A field the
// result changed and the other change did not takes the result's value; a
// field both changed keeps the other change, which happened later.
func mergeState(before, ours, theirs model.CheckState) model.CheckState {
	out := theirs
	b, o, t := reflect.ValueOf(before), reflect.ValueOf(ours), reflect.ValueOf(theirs)
	dst := reflect.ValueOf(&out).Elem()
	for i := 0; i < b.NumField(); i++ {
		if reflect.DeepEqual(o.Field(i).Interface(), b.Field(i).Interface()) {
			continue // the result did not change it
		}
		if !reflect.DeepEqual(t.Field(i).Interface(), b.Field(i).Interface()) {
			continue // changed by both: the later change stands
		}
		dst.Field(i).Set(o.Field(i))
	}
	return out
}
