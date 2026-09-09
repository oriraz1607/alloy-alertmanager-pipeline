package alertmanager

import (
	"context"
	"sync"
)

// Receiver accepts typed alerts from upstream Alloy components.
//
// Send returns after the receiver accepts or rejects the complete collection.
// Implementations must honor context cancellation while applying backpressure.
type Receiver interface {
	Send(context.Context, []Alert) error
}

// ReceiverFunc adapts a function into a Receiver.
type ReceiverFunc func(context.Context, []Alert) error

// Send implements Receiver.
func (f ReceiverFunc) Send(ctx context.Context, alerts []Alert) error {
	return f(ctx, alerts)
}

// Fanout distributes alerts to a dynamically updateable list of receivers.
type Fanout struct {
	mut      sync.RWMutex
	children []Receiver
}

// NewFanout creates a Fanout for children.
func NewFanout(children []Receiver) *Fanout {
	return &Fanout{children: children}
}

// Send forwards the complete collection to each configured receiver.
func (f *Fanout) Send(ctx context.Context, alerts []Alert) error {
	// Hold the read lock through delivery so an Alloy configuration update can't
	// remove a receiver while an in-flight webhook is sending to it.
	f.mut.RLock()
	defer f.mut.RUnlock()

	for _, recv := range f.children {
		if err := recv.Send(ctx, CloneAlerts(alerts)); err != nil {
			return err
		}
	}
	return nil
}

// UpdateChildren atomically replaces the receivers used by future sends.
func (f *Fanout) UpdateChildren(children []Receiver) {
	f.mut.Lock()
	f.children = children
	f.mut.Unlock()
}

// Clear removes all receivers after all in-flight sends finish.
func (f *Fanout) Clear() {
	f.UpdateChildren(nil)
}
