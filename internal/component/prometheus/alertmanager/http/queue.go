package http

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"time"
)

var (
	errQueueFull = errors.New("JSON delivery queue is full")
	errStopped   = errors.New("HTTP sender is stopping")
)

type payloadQueue struct {
	mut      sync.Mutex
	items    []queuedPayload
	inFlight int
	capacity int
	stopped  bool
	changed  chan struct{}
	space    chan struct{}
}

type queuedPayload struct {
	body        []byte
	retries     int
	nextAttempt time.Time
}

func newPayloadQueue(capacity int) *payloadQueue {
	return &payloadQueue{capacity: capacity, changed: make(chan struct{}, 1), space: make(chan struct{}, 1)}
}

func (q *payloadQueue) enqueue(ctx context.Context, bodies [][]byte, block bool) error {
	if len(bodies) == 0 {
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
		if len(bodies) > q.capacity {
			q.mut.Unlock()
			return errQueueFull
		}
		if len(bodies) <= q.capacity-len(q.items)-q.inFlight {
			for _, body := range bodies {
				q.items = append(q.items, queuedPayload{body: bytes.Clone(body)})
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

func (q *payloadQueue) takeReady(now time.Time) (queuedPayload, time.Duration, bool) {
	q.mut.Lock()
	defer q.mut.Unlock()
	if len(q.items) == 0 {
		return queuedPayload{}, 0, false
	}

	readyIndex := -1
	var wait time.Duration
	for index, item := range q.items {
		if item.nextAttempt.IsZero() || !item.nextAttempt.After(now) {
			readyIndex = index
			break
		}
		itemWait := item.nextAttempt.Sub(now)
		if wait == 0 || itemWait < wait {
			wait = itemWait
		}
	}
	if readyIndex < 0 {
		return queuedPayload{}, wait, false
	}

	item := q.items[readyIndex]
	copy(q.items[readyIndex:], q.items[readyIndex+1:])
	q.items = q.items[:len(q.items)-1]
	if len(q.items) == 0 {
		q.items = nil
	}
	q.inFlight++
	return item, 0, true
}

func (q *payloadQueue) retry(item queuedPayload, nextAttempt time.Time) {
	q.mut.Lock()
	item.nextAttempt = nextAttempt
	q.items = append(q.items, item)
	q.inFlight--
	q.mut.Unlock()
	q.notify()
}

func (q *payloadQueue) complete() {
	q.mut.Lock()
	q.inFlight--
	q.mut.Unlock()
	q.notify()
}

func (q *payloadQueue) len() int {
	q.mut.Lock()
	defer q.mut.Unlock()
	return len(q.items) + q.inFlight
}

func (q *payloadQueue) setCapacity(capacity int) {
	q.mut.Lock()
	q.capacity = capacity
	q.mut.Unlock()
	q.notify()
}

func (q *payloadQueue) stop() {
	q.mut.Lock()
	q.stopped = true
	q.mut.Unlock()
	q.notify()
}

func (q *payloadQueue) discard() int {
	q.mut.Lock()
	defer q.mut.Unlock()
	count := len(q.items) + q.inFlight
	q.items = nil
	q.inFlight = 0
	q.notify()
	return count
}

func (q *payloadQueue) notify() {
	select {
	case q.space <- struct{}{}:
	default:
	}
	select {
	case q.changed <- struct{}{}:
	default:
	}
}
