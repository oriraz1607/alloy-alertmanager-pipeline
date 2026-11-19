// Package route dispatches typed alerts to receivers based on alert labels.
package route

import (
	"context"
	"sync"

	"github.com/grafana/alloy/internal/component"
	alertpipeline "github.com/grafana/alloy/internal/component/common/alertmanager"
)

func init() {
	component.Register(component.Registration{
		Name:      "prometheus.alertmanager.route",
		Community: true,
		Args:      Arguments{},
		Exports:   Exports{},
		Build: func(opts component.Options, args component.Arguments) (component.Component, error) {
			return New(opts, args.(Arguments))
		},
	})
}

// Exports holds the stable receiver used by upstream components.
type Exports struct {
	Receiver alertpipeline.Receiver `alloy:"receiver,attr"`
}

// Component dispatches typed alerts using a reloadable routing table.
type Component struct {
	id    string
	mut   sync.Mutex
	table *routingTable
}

type routingTable struct {
	routes    []compiledRoute
	defaults  []int
	receivers []alertpipeline.Receiver // First occurrence in configuration order.
	inFlight  sync.WaitGroup
}

// New installs the initial routing table before publishing the receiver.
func New(opts component.Options, args Arguments) (*Component, error) {
	c := &Component{id: opts.ID}
	if err := c.Update(args); err != nil {
		return nil, err
	}
	opts.OnStateChange(Exports{Receiver: c})
	return c, nil
}

func (c *Component) String() string { return c.id + ".receiver" }

// Run waits for shutdown. The router has no queue or background workers.
func (c *Component) Run(ctx context.Context) error {
	<-ctx.Done()
	return nil
}

// Update replaces the active table only after the entire configuration compiles.
func (c *Component) Update(args component.Arguments) error {
	table, err := compile(args.(Arguments))
	if err != nil {
		return err
	}
	c.mut.Lock()
	old := c.table
	c.table = table
	c.mut.Unlock()
	// Keep the old receivers admitted to the pipeline until their last Send
	// finishes. New sends can already use the replacement with their own deadlines.
	if old != nil {
		old.inFlight.Wait()
	}
	return nil
}

// Send admits each destination's ordered batch under the caller's context.
func (c *Component) Send(ctx context.Context, alerts []alertpipeline.Alert) error {
	c.mut.Lock()
	table := c.table
	table.inFlight.Add(1)
	c.mut.Unlock()
	defer table.inFlight.Done()

	batches := make([][]int, len(table.receivers))
	for i, alert := range alerts {
		if err := ctx.Err(); err != nil {
			return err
		}
		matched := false
		for _, route := range table.routes {
			if !route.matchers.Matches(alert.Labels) {
				continue
			}
			matched = true
			selectAlert(batches, route.destinations, i)
			if !route.continueMatching {
				break
			}
		}
		if !matched {
			selectAlert(batches, table.defaults, i)
		}
	}

	for dest, indexes := range batches {
		if len(indexes) == 0 {
			continue
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		batch := make([]alertpipeline.Alert, len(indexes))
		for i, index := range indexes {
			batch[i] = alerts[index].Clone()
		}
		if err := table.receivers[dest].Send(ctx, batch); err != nil {
			return err
		}
	}
	return nil
}

func selectAlert(batches [][]int, destinations []int, index int) {
	for _, dest := range destinations {
		batch := batches[dest]
		// Input positions increase monotonically. A repeated selection of
		// this alert can only refer to the last position in this batch.
		if len(batch) == 0 || batch[len(batch)-1] != index {
			batches[dest] = append(batch, index)
		}
	}
}
