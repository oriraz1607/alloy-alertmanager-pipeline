package http

import (
	"bytes"
	"context"
	"errors"
	"sync"
)

var (
	errQueueFull = errors.New("JSON delivery queue is full")
	errStopped   = errors.New("HTTP sender is stopping")
)

type payloadQueue struct {
	mut      sync.Mutex
	items    [][]byte
	capacity int
	stopped  bool
	changed  chan struct{}
	space    chan struct{}
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
		if len(bodies) <= q.capacity-len(q.items) {
			for _, body := range bodies {
				q.items = append(q.items, bytes.Clone(body))
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

func (q *payloadQueue) dequeue() []byte {
	q.mut.Lock()
	defer q.mut.Unlock()
	if len(q.items) == 0 {
		return nil
	}
	body := q.items[0]
	q.items = q.items[1:]
	if len(q.items) == 0 {
		q.items = nil
	}
	q.notify()
	return body
}

func (q *payloadQueue) prepend(body []byte) {
	if len(body) == 0 {
		return
	}
	q.mut.Lock()
	q.items = append([][]byte{bytes.Clone(body)}, q.items...)
	q.mut.Unlock()
	q.notify()
}

func (q *payloadQueue) len() int {
	q.mut.Lock()
	defer q.mut.Unlock()
	return len(q.items)
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
