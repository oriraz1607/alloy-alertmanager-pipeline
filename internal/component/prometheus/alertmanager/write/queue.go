package write

import (
	"context"
	"errors"
	"sync"

	alertpipeline "github.com/grafana/alloy/internal/component/common/alertmanager"
)

var (
	errQueueFull = errors.New("alert queue is full")
	errStopped   = errors.New("alert writer is stopping")
)

type alertQueue struct {
	store    *queueStore
	inflight []alertpipeline.Alert
	mut      sync.Mutex
	items    []alertpipeline.Alert
	capacity int
	stopped  bool
	changed  chan struct{}
	space    chan struct{}
}

func newAlertQueue(capacity int) *alertQueue {
	return &alertQueue{capacity: capacity, changed: make(chan struct{}, 1), space: make(chan struct{}, 1)}
}

func (q *alertQueue) enqueue(ctx context.Context, alerts []alertpipeline.Alert, block bool, onAccepted func()) error {
	if len(alerts) == 0 {
		return nil
	}
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		q.mut.Lock()
		if q.stopped {
			q.mut.Unlock()
			return errStopped
		}
		if len(alerts) > q.capacity {
			q.mut.Unlock()
			return errQueueFull
		}
		if len(alerts) <= q.capacity-len(q.items)-len(q.inflight) {
			next := append(alertpipeline.CloneAlerts(q.items), alertpipeline.CloneAlerts(alerts)...)
			if q.store != nil {
				if err := q.store.save(append(alertpipeline.CloneAlerts(q.inflight), next...), alerts); err != nil {
					q.mut.Unlock()
					// Disk exhaustion is rejected explicitly, even with blocking overflow:
					// tracked refresh state may occupy the space indefinitely.
					return err
				}
			}
			q.items = next
			if onAccepted != nil {
				onAccepted()
			}
			q.mut.Unlock()
			q.notify()
			return nil
		}
		q.mut.Unlock()

		if !block {
			return errQueueFull
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-q.space:
		}
	}
}

func (q *alertQueue) dequeue(maxItems int) []alertpipeline.Alert {
	q.mut.Lock()
	defer q.mut.Unlock()
	if len(q.items) == 0 {
		return nil
	}
	count := min(maxItems, len(q.items))
	items := q.items[:count]
	if q.store != nil {
		q.inflight = append(q.inflight, items...)
	}
	q.items = q.items[count:]
	if len(q.items) == 0 {
		q.items = nil
	}
	q.notify()
	return items
}

func (q *alertQueue) prepend(alerts []alertpipeline.Alert) {
	if len(alerts) == 0 {
		return
	}
	q.mut.Lock()
	if q.store != nil {
		q.inflight = nil
	}
	q.items = append(alertpipeline.CloneAlerts(alerts), q.items...)
	q.mut.Unlock()
	q.notify()
}

func (q *alertQueue) len() int {
	q.mut.Lock()
	defer q.mut.Unlock()
	return len(q.items)
}

func (q *alertQueue) setCapacity(capacity int) {
	q.mut.Lock()
	q.capacity = capacity
	q.mut.Unlock()
	q.notify()
}

func (q *alertQueue) stop() {
	q.mut.Lock()
	q.stopped = true
	q.mut.Unlock()
	q.notify()
}

func (q *alertQueue) notify() {
	// Separate wakeups prevent the sender from consuming a producer's capacity
	// notification while it waits for new work.
	select {
	case q.space <- struct{}{}:
	default:
	}

	select {
	case q.changed <- struct{}{}:
	default:
	}
}

// acknowledge removes the sole sender's in-flight batch only after HTTP delivery
// or an explicitly terminal response. Failure keeps the committed record intact.
func (q *alertQueue) acknowledge() error {
	q.mut.Lock()
	defer q.mut.Unlock()
	if q.store == nil {
		return nil
	}
	if err := q.store.save(q.items, nil); err != nil {
		return err
	}
	q.inflight = nil
	q.notify()
	return nil
}
