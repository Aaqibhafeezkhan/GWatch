package engine

// The alert outbox: alerts that could not be delivered when they were raised
// wait in the database (store.QueuedAlert) and are sent once the channel
// works again.
//
// The case it exists for is the commonest outage on a home network: the
// router or the internet connection goes down, and with it every way GWatch
// has of saying so. Before the outbox, a down alert that failed was re-armed
// and sent again only if the check was still failing; an outage that was over
// before the connection came back was never reported at all, and a recovery
// or warning that failed was simply lost.
//
// The rules:
//
//   - An alert is tried as it always was (with the one quick retry). Each
//     destination that still fails — the e-mail, or a Settings › Alerts
//     channel — gets a row in the outbox and is tried again after 1, 2, 4, 8
//     and then every 15 minutes, or later if the service sent Retry-After.
//     After a day it is given up on, and the timeline says so.
//   - A queued alert counts as raised: the check's alert stays active and its
//     cooldown runs, so the next failing result does not raise a second one.
//   - A destination's alerts go out in the order they were raised. While one
//     is waiting, newer alerts for the same destination queue behind it, so a
//     recovery cannot arrive before the outage it ends.
//   - A down alert and its recovery that are both still waiting are sent as
//     one message ("down from 14:02 to 14:20"), except to PagerDuty and
//     Opsgenie, which need the two events to open and close an incident.
//   - Everything sent late says when it was raised.

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/jxburros/GWatch/internal/mailer"
	"github.com/jxburros/GWatch/internal/model"
	"github.com/jxburros/GWatch/internal/store"
)

const (
	// outboxGiveUp is how long a queued alert is tried for.
	outboxGiveUp = 24 * time.Hour
	// outboxRetryMost is the longest wait between attempts.
	outboxRetryMost = 15 * time.Minute
	// outboxPoll is how often the outbox is looked at.
	outboxPoll = 30 * time.Second
	// emailTarget is the outbox destination of an alert e-mail; a channel's
	// is channelTarget(id).
	emailTarget = "email"
)

func channelTarget(id string) string { return "channel:" + id }

// outboxBackoff is the wait after the n-th failed attempt (n >= 1).
func outboxBackoff(n int) time.Duration {
	d := time.Minute
	for i := 1; i < n && d < outboxRetryMost; i++ {
		d *= 2
	}
	return min(d, outboxRetryMost)
}

// alertPayload is a queued alert as it is stored: what is needed to send it
// again, without the node's or check's configuration (which can hold
// secrets, and which is read afresh when the alert is sent).
type alertPayload struct {
	Kind         string            `json:"kind"`
	NodeID       int64             `json:"nodeId"`
	NodeName     string            `json:"nodeName"`
	CheckID      int64             `json:"checkId"`
	CheckName    string            `json:"checkName"`
	CheckType    model.CheckType   `json:"checkType,omitempty"`
	Status       model.Status      `json:"status"`
	Message      string            `json:"message"`
	Details      map[string]string `json:"details,omitempty"`
	To           []string          `json:"to,omitempty"`
	Result       model.Result      `json:"result"`
	State        model.CheckState  `json:"state"`
	ChannelsOnly bool              `json:"channelsOnly,omitempty"`
	Group        []alertPayload    `json:"group,omitempty"`
}

func payloadOf(m mailTask) alertPayload {
	p := alertPayload{
		Kind: m.kind, NodeID: m.node.ID, NodeName: m.node.Name, CheckID: m.check.ID, CheckName: m.check.Name, CheckType: m.check.Type,
		Status: m.status, Message: m.message, Details: m.details, To: m.to, Result: m.result, State: m.state, ChannelsOnly: m.channelsOnly,
	}
	for _, t := range m.group {
		p.Group = append(p.Group, payloadOf(t))
	}
	return p
}

// taskOf turns a stored alert back into one that can be sent, with the
// node's and check's current settings where they still exist. It reports
// false when the check has been deleted since (an alert about a remote
// collector, whose check id is negative, has no check to lose).
func (e *Engine) taskOf(p alertPayload) (mailTask, bool) {
	e.mu.Lock()
	n, haveNode := e.nodes[p.NodeID]
	c, haveCheck := e.checks[p.CheckID]
	e.mu.Unlock()
	if !haveCheck {
		if p.CheckID > 0 && len(p.Group) == 0 {
			return mailTask{}, false
		}
		c = model.Check{ID: p.CheckID, NodeID: p.NodeID, Name: p.CheckName, Type: p.CheckType}
	}
	if !haveNode {
		n = model.Node{ID: p.NodeID, Name: p.NodeName}
	}
	m := mailTask{kind: p.Kind, check: c, node: n, status: p.Status, message: p.Message, details: p.Details, to: p.To, result: p.Result, state: p.State, channelsOnly: p.ChannelsOnly}
	for _, g := range p.Group {
		if t, ok := e.taskOf(g); ok {
			m.group = append(m.group, t)
		}
	}
	if len(p.Group) > 0 && len(m.group) == 0 {
		return mailTask{}, false
	}
	if m.details == nil {
		m.details = map[string]string{}
	}
	return m, true
}

// outboxAlert puts one destination of an alert in the outbox. lastErr is why
// it could not be sent (empty when it is only queued behind earlier alerts),
// and wait how long the service asked to be left alone.
func (e *Engine) outboxAlert(m mailTask, target string, raised time.Time, lastErr string, wait time.Duration) bool {
	raw, err := json.Marshal(payloadOf(m))
	if err != nil {
		e.log.Errorf("queue alert: %v", err)
		return false
	}
	next, attempts := raised, 0
	if lastErr != "" {
		attempts = 1
		next = raised.Add(max(outboxBackoff(1), wait))
	}
	if _, err := e.store.QueueAlert(e.workContext(), store.QueuedAlert{CheckID: m.check.ID, Target: target, Kind: m.kind, CreatedAt: raised, NextAt: next, Attempts: attempts, LastError: lastErr, Payload: string(raw)}); err != nil {
		e.log.Errorf("queue alert for %s/%s: %v", m.node.Name, m.check.Name, err)
		return false
	}
	return true
}

// backlog is the set of destinations that already have alerts waiting.
func (e *Engine) backlog() map[string]bool {
	q, err := e.store.QueuedAlerts(e.workContext())
	if err != nil {
		e.log.Errorf("read the alert outbox: %v", err)
		return nil
	}
	out := map[string]bool{}
	for _, a := range q {
		out[a.Target] = true
	}
	return out
}

// recordAbout records an event on the timeline of each check an alert is
// about: one for an ordinary alert, every member's for a grouped one.
func (e *Engine) recordAbout(m mailTask, t model.EventType, title, detail string) {
	about := m.group
	if len(about) == 0 {
		about = []mailTask{m}
	}
	for _, a := range about {
		e.recordEvent(model.Event{Type: t, NodeID: ptrInt64(a.node.ID), CheckID: ptrInt64(a.check.ID), NodeName: a.node.Name, CheckName: a.check.Name, Title: title, Detail: detail})
	}
}

// outboxState is what Monitor health shows about the outbox.
func (e *Engine) outboxState(ctx context.Context) (int, *time.Time) {
	q, err := e.store.QueuedAlerts(ctx)
	if err != nil || len(q) == 0 {
		return 0, nil
	}
	oldest := q[0].CreatedAt
	for _, a := range q {
		if a.CreatedAt.Before(oldest) {
			oldest = a.CreatedAt
		}
	}
	return len(q), &oldest
}

// outboxLoop drains the outbox while the engine runs.
func (e *Engine) outboxLoop() {
	defer e.wg.Done()
	t := time.NewTicker(outboxPoll)
	defer t.Stop()
	if e.enterWork() {
		e.drainOutbox(e.workContext(), time.Now())
		e.leaveWork()
	}
	for {
		select {
		case <-e.ctx.Done():
			return
		case now := <-t.C:
			if e.enterWork() {
				e.drainOutbox(e.workContext(), now)
				e.leaveWork()
			}
		}
	}
}

// drainOutbox sends the queued alerts that are due, each destination's in
// the order they were raised, stopping at the first one per destination that
// is not due or fails again.
func (e *Engine) drainOutbox(ctx context.Context, now time.Time) {
	if !e.outboxMu.TryLock() {
		return
	}
	defer e.outboxMu.Unlock()
	queued, err := e.store.QueuedAlerts(ctx)
	if err != nil {
		e.log.Errorf("read the alert outbox: %v", err)
		return
	}
	if len(queued) == 0 {
		return
	}
	e.mu.Lock()
	s := e.settings
	e.mu.Unlock()
	byTarget := map[string][]store.QueuedAlert{}
	var order []string
	for _, q := range queued {
		if _, seen := byTarget[q.Target]; !seen {
			order = append(order, q.Target)
		}
		byTarget[q.Target] = append(byTarget[q.Target], q)
	}
	for _, target := range order {
		e.drainTarget(ctx, now, s, target, byTarget[target])
	}
}

func (e *Engine) drainTarget(ctx context.Context, now time.Time, s model.Settings, target string, items []store.QueuedAlert) {
	var ch *model.AlertChannel
	if id, ok := strings.CutPrefix(target, "channel:"); ok {
		for i := range s.Alerts.Channels {
			if s.Alerts.Channels[i].ID == id {
				ch = &s.Alerts.Channels[i]
			}
		}
	}
	// Alerts switched off, or the channel removed or switched off since:
	// what was waiting for it is dropped, and the timeline says so.
	gone := !s.Alerts.Enabled || (target != emailTarget && (ch == nil || !ch.Enabled))
	for i := 0; i < len(items); i++ {
		q := items[i]
		var p alertPayload
		if err := json.Unmarshal([]byte(q.Payload), &p); err != nil {
			e.log.Errorf("unreadable queued alert %d: %v", q.ID, err)
			e.dropQueued(ctx, q.ID)
			continue
		}
		m, ok := e.taskOf(p)
		if !ok {
			e.dropQueued(ctx, q.ID) // the check has been deleted
			continue
		}
		label := alertLabel(m.kind)
		if gone {
			e.dropQueued(ctx, q.ID)
			e.recordAbout(m, model.EventAlertFailed, label+" not sent late", "It was waiting to be sent again, but alerts or this destination have been switched off since.")
			continue
		}
		if now.Sub(q.CreatedAt) > outboxGiveUp {
			e.dropQueued(ctx, q.ID)
			e.recordAbout(m, model.EventAlertFailed, label+" given up", fmt.Sprintf("It could not be sent for a day after it was raised at %s. Last error: %s", q.CreatedAt.Local().Format("2006-01-02 15:04"), firstLine(q.LastError)))
			continue
		}
		if q.NextAt.After(now) {
			return // not due; nothing behind it may overtake it
		}
		// A down alert and its recovery, both still waiting: one message.
		merged := []int64{q.ID}
		if m.kind == "down" && (ch == nil || !ch.Action.Type.IsIncident()) {
			for j := i + 1; j < len(items); j++ {
				if items[j].CheckID == q.CheckID && items[j].Kind == "recovered" {
					var rp alertPayload
					if json.Unmarshal([]byte(items[j].Payload), &rp) != nil {
						break
					}
					if r, ok := e.taskOf(rp); ok {
						m = outageSummary(m, r, q.CreatedAt)
						merged = append(merged, items[j].ID)
						items = append(items[:j:j], items[j+1:]...)
					}
					break
				}
			}
		}
		if len(merged) == 1 {
			lateNote(&m, q.CreatedAt, now, e.statusNow(m))
		}
		res := e.sendQueued(ctx, s, target, ch, m)
		if !res.OK {
			wait := max(outboxBackoff(q.Attempts+1), res.RetryAfter)
			if err := e.store.RescheduleQueuedAlert(ctx, q.ID, q.Attempts+1, now.Add(wait), res.Error); err != nil {
				e.log.Errorf("reschedule queued alert %d: %v", q.ID, err)
			}
			return // the destination is still not working; try it later
		}
		for _, id := range merged {
			e.dropQueued(ctx, id)
		}
		where := "by e-mail"
		if ch != nil {
			where = "to " + channelName(*ch)
		}
		e.recordAbout(m, model.EventAlertSent, fmt.Sprintf("%s sent late %s", label, where), fmt.Sprintf("Raised at %s and sent once the destination was working again.", q.CreatedAt.Local().Format("15:04")))
	}
}

func (e *Engine) dropQueued(ctx context.Context, id int64) {
	if err := e.store.DeleteQueuedAlert(ctx, id); err != nil {
		e.log.Errorf("remove queued alert %d: %v", id, err)
	}
}

// statusNow is what the check an alert is about is doing now, for the note
// on an alert sent late.
func (e *Engine) statusNow(m mailTask) model.Status {
	if len(m.group) > 0 {
		return ""
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if st := e.states[m.check.ID]; st != nil {
		return st.Status
	}
	return ""
}

// lateNote marks an alert as sent late, saying when it was raised and, when
// it has changed since, what the check is doing now.
func lateNote(m *mailTask, raised, now time.Time, status model.Status) {
	note := fmt.Sprintf("Raised at %s; it could not be sent at the time.", raised.Local().Format("2006-01-02 15:04"))
	if status != "" && status != m.status {
		note += fmt.Sprintf(" The check is %s now.", status)
	}
	m.details = cloneDetails(m.details)
	m.details["Sent late"] = note
	m.message = strings.TrimSpace(m.message + " (sent late: raised at " + raised.Local().Format("15:04") + ")")
}

// outageSummary is the one message sent for a down alert and its recovery
// that were both still waiting: a recovery that says how long it was down.
func outageSummary(down, recovered mailTask, raised time.Time) mailTask {
	m := recovered
	from, to := down.result.Timestamp, recovered.result.Timestamp
	if from.IsZero() {
		from = raised
	}
	m.message = fmt.Sprintf("Was down from %s to %s (%s). Neither alert could be sent at the time.", from.Local().Format("15:04"), to.Local().Format("15:04"), humanDuration(to.Sub(from)))
	m.details = cloneDetails(recovered.details)
	m.details["Down from"] = from.Local().Format("2006-01-02 15:04:05")
	m.details["Recovered at"] = to.Local().Format("2006-01-02 15:04:05")
	if why := down.details["Error"]; why != "" {
		m.details["Error"] = why
	}
	return m
}

func cloneDetails(d map[string]string) map[string]string {
	out := make(map[string]string, len(d)+1)
	for k, v := range d {
		out[k] = v
	}
	return out
}

func channelName(ch model.AlertChannel) string {
	if strings.TrimSpace(ch.Name) != "" {
		return ch.Name
	}
	return string(ch.Action.Type)
}

// sendQueued makes one attempt to send a queued alert to its destination.
func (e *Engine) sendQueued(ctx context.Context, s model.Settings, target string, ch *model.AlertChannel, m mailTask) model.ActionResult {
	if target == emailTarget {
		if len(m.to) == 0 {
			return model.ActionResult{Error: "no recipients"}
		}
		msg := alertEmail(m, s.General.InstanceName)
		msg.To = m.to
		err := e.sendEmailOnce(ctx, s.Alerts.SMTP, msg)
		e.mu.Lock()
		e.lastAlertAt = ptrTime(time.Now())
		if err != nil {
			e.lastAlertErr = err.Error()
		} else {
			e.lastAlertErr = ""
		}
		e.mu.Unlock()
		if err != nil {
			return model.ActionResult{Error: err.Error()}
		}
		return model.ActionResult{OK: true}
	}
	if ch == nil {
		return model.ActionResult{Error: "channel not found"}
	}
	vars := alertVars(m, s.General.InstanceName, channelName(*ch))
	if late := m.details["Sent late"]; late != "" {
		vars["alert.late"] = late
	}
	return e.runChannelOnce(ctx, *ch, vars)
}

// alertEmail builds the e-mail for an alert: a grouped one for a group.
func alertEmail(m mailTask, instance string) mailer.Message {
	if len(m.group) == 0 {
		return mailer.BuildAlertEmail(m.kind, instance, m.node.Name, m.check.Name, m.status, m.message, m.details, m.result.Timestamp)
	}
	items := make([]mailer.AlertItem, 0, len(m.group))
	for _, t := range m.group {
		items = append(items, mailer.AlertItem{NodeName: t.node.Name, CheckName: t.check.Name, Status: t.status, Message: t.message, Details: t.details, When: t.result.Timestamp})
	}
	msg := mailer.BuildGroupedAlertEmail(m.kind, instance, m.node.Name, items, m.group[0].result.Timestamp)
	if late := m.details["Sent late"]; late != "" {
		msg.Subject += " (sent late)"
	}
	return msg
}

// sendEmailOnce sends one e-mail through the shared slots, without the
// quick retry deliverMail makes: the outbox does its own retrying.
func (e *Engine) sendEmailOnce(ctx context.Context, smtp model.SMTPSettings, msg mailer.Message) error {
	select {
	case e.mailSem <- struct{}{}:
	case <-ctx.Done():
		return ctx.Err()
	}
	defer func() { <-e.mailSem }()
	ctx, cancel := context.WithTimeout(ctx, 45*time.Second)
	defer cancel()
	return e.opts.Send(ctx, smtp, msg)
}

// runChannelOnce runs one channel through the shared slots.
func (e *Engine) runChannelOnce(ctx context.Context, ch model.AlertChannel, vars map[string]string) model.ActionResult {
	select {
	case e.mailSem <- struct{}{}:
	case <-ctx.Done():
		return model.ActionResult{Error: ctx.Err().Error()}
	}
	defer func() { <-e.mailSem }()
	return e.Actions().Run(ctx, ch.Action, vars)
}
