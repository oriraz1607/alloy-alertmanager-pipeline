package alertmanager

import (
	"context"
	"fmt"
	"reflect"
	"sync"
)

// Receiver accepts typed alerts from upstream Alloy components.
//
// Send returns after the receiver accepts or rejects the complete collection.
// Implementations must honor context cancellation while applying backpressure.
type Receiver interface {
	Send(context.Context, []Alert) error
}

// ValidateReceivers checks that a destination list can accept forwarded alerts.
func ValidateReceivers(receivers []Receiver) error {
	if len(receivers) == 0 {
		return fmt.Errorf("must contain at least one receiver")
	}
	for i, receiver := range receivers {
		v := reflect.ValueOf(receiver)
		switch v.Kind() {
		case reflect.Invalid:
			return fmt.Errorf("receiver %d is nil", i+1)
		case reflect.Chan, reflect.Func, reflect.Map, reflect.Pointer, reflect.Slice:
			if v.IsNil() {
				return fmt.Errorf("receiver %d is nil", i+1)
			}
		}
	}
	return nil
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
