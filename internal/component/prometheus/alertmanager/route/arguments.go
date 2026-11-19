package route

import (
	"fmt"
	"reflect"

	"github.com/prometheus/alertmanager/matcher/parse"
	"github.com/prometheus/alertmanager/pkg/labels"

	alertpipeline "github.com/grafana/alloy/internal/component/common/alertmanager"
)

// Arguments configures prometheus.alertmanager.route.
type Arguments struct {
	Routes           []Route                  `alloy:"route,block,optional"`
	DefaultForwardTo []alertpipeline.Receiver `alloy:"default_forward_to,attr"`
}

// Route selects receivers when all of its matchers match an alert.
type Route struct {
	Matchers  []string                 `alloy:"matchers,attr"`
	ForwardTo []alertpipeline.Receiver `alloy:"forward_to,attr"`
	Continue  bool                     `alloy:"continue,attr,optional"`
}

type compiledRoute struct {
	matchers         labels.Matchers
	destinations     []int
	continueMatching bool
}

// Validate implements syntax.Validator.
func (args *Arguments) Validate() error {
	_, err := compile(*args)
	return err
}

func compile(args Arguments) (*routingTable, error) {
	table := &routingTable{}
	// Key by the exported receiver object. HTTP, write, and route export
	// pointers. reflect.Value also accommodates function-backed receivers,
	// which cannot be compared directly or used as interface map keys.
	receiverIDs := make(map[reflect.Value]int)
	destinations := func(receivers []alertpipeline.Receiver) ([]int, error) {
		if len(receivers) == 0 {
			return nil, fmt.Errorf("must contain at least one receiver")
		}
		ids := make([]int, 0, len(receivers))
		for i, receiver := range receivers {
			key := reflect.ValueOf(receiver)
			if !key.IsValid() || ((key.Kind() == reflect.Pointer || key.Kind() == reflect.Func) && key.IsNil()) {
				return nil, fmt.Errorf("receiver %d is nil", i+1)
			}
			id, ok := receiverIDs[key]
			if !ok {
				id = len(table.receivers)
				receiverIDs[key] = id
				table.receivers = append(table.receivers, receiver)
			}
			ids = append(ids, id)
		}
		return ids, nil
	}

	for i, route := range args.Routes {
		ids, err := destinations(route.ForwardTo)
		if err != nil {
			return nil, fmt.Errorf("route %d forward_to: %w", i+1, err)
		}
		compiled := compiledRoute{destinations: ids, continueMatching: route.Continue}
		for _, expr := range route.Matchers {
			m, err := parse.Matcher(expr)
			if err != nil {
				return nil, fmt.Errorf("route %d matcher %q: %w", i+1, expr, err)
			}
			compiled.matchers = append(compiled.matchers, m)
		}
		table.routes = append(table.routes, compiled)
	}
	ids, err := destinations(args.DefaultForwardTo)
	if err != nil {
		return nil, fmt.Errorf("default_forward_to: %w", err)
	}
	table.defaults = ids
	return table, nil
}
