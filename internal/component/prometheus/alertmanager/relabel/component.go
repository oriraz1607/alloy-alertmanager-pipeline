// Package relabel applies Prometheus relabel rules to typed Alertmanager alerts.
package relabel

import (
	"context"
	"fmt"
	"slices"
	"sync"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	promrelabel "github.com/prometheus/prometheus/model/relabel"

	"github.com/grafana/alloy/internal/component"
	alertpipeline "github.com/grafana/alloy/internal/component/common/alertmanager"
	alloyrelabel "github.com/grafana/alloy/internal/component/common/relabel"
)

func init() {
	component.Register(component.Registration{
		Name:      "prometheus.alertmanager.relabel",
		Community: true,
		Args:      Arguments{},
		Exports:   Exports{},
		Build: func(opts component.Options, args component.Arguments) (component.Component, error) {
			return New(opts, args.(Arguments))
		},
	})
}

// Arguments configures the ordered rules and destinations.
type Arguments struct {
	ForwardTo []alertpipeline.Receiver `alloy:"forward_to,attr"`
	Rules     []*alloyrelabel.Config   `alloy:"rule,block,optional"`
}

// Validate implements syntax.Validator.
func (args *Arguments) Validate() error {
	_, err := args.compileRules()
	return err
}

func (args *Arguments) compileRules() ([]*promrelabel.Config, error) {
	for i, rule := range args.Rules {
		if rule == nil || rule.Regex.Regexp == nil {
			return nil, fmt.Errorf("rule %d: missing rule or compiled regex", i+1)
		}
		// Source labels read existing alert names, including valid UTF-8 names.
		for _, source := range rule.SourceLabels {
			if !model.UTF8Validation.IsValidLabelName(source) {
				return nil, fmt.Errorf("rule %d: invalid source label %q", i+1, source)
			}
		}
		// Alloy decodes action names with UnmarshalText. Apply the same check
		// when Update receives an Arguments value from a Go caller.
		var action alloyrelabel.Action
		if err := action.UnmarshalText([]byte(rule.Action)); err != nil {
			return nil, fmt.Errorf("rule %d: %w", i+1, err)
		}
	}
	rules := alloyrelabel.ComponentToPromRelabelConfigs(args.Rules)
	for i, rule := range rules {
		// Prometheus compares the default regex by pointer for the equality actions.
		if rule.Regex.String() == promrelabel.DefaultRelabelConfig.Regex.String() {
			rule.Regex = promrelabel.DefaultRelabelConfig.Regex
		}
		if err := rule.Validate(model.LegacyValidation); err != nil {
			return nil, fmt.Errorf("rule %d: %w", i+1, err)
		}
	}
	return rules, nil
}

// Exports holds the stable typed receiver for upstream components.
type Exports struct {
	Receiver alertpipeline.Receiver `alloy:"receiver,attr"`
}

// Component processes each collection using a single configuration.
type Component struct {
	id    string
	mut   sync.Mutex
	state *state
}

type state struct {
	rules    []*promrelabel.Config
	fanout   *alertpipeline.Fanout
	inFlight sync.WaitGroup
}

// New installs the initial rules before publishing the receiver.
func New(opts component.Options, args Arguments) (*Component, error) {
	c := &Component{id: opts.ID}
	if err := c.Update(args); err != nil {
		return nil, err
	}
	opts.OnStateChange(Exports{Receiver: c})
	return c, nil
}

func (c *Component) String() string { return c.id + ".receiver" }

// Run waits for shutdown. Relabeling has no background workers.
func (c *Component) Run(ctx context.Context) error {
	<-ctx.Done()
	return nil
}

// Update activates the new rules and destinations together.
func (c *Component) Update(args component.Arguments) error {
	newArgs := args.(Arguments)
	rules, err := newArgs.compileRules()
	if err != nil {
		return err
	}
	next := &state{
		rules:  rules,
		fanout: alertpipeline.NewFanout(slices.Clone(newArgs.ForwardTo)),
	}
	c.mut.Lock()
	old := c.state
	c.state = next
	c.mut.Unlock()

	// Retire the old destinations only after their last sender returns.
	// New arrivals can already use the new configuration.
	if old != nil {
		old.inFlight.Wait()
	}
	return nil
}

// Send relabels each alert and forwards one collection of survivors.
func (c *Component) Send(ctx context.Context, alerts []alertpipeline.Alert) error {
	c.mut.Lock()
	current := c.state
	current.inFlight.Add(1)
	c.mut.Unlock()
	defer current.inFlight.Done()

	kept := make([]alertpipeline.Alert, 0, len(alerts))
	for i, alert := range alerts {
		if err := ctx.Err(); err != nil {
			return err
		}
		if len(current.rules) > 0 {
			relabeled, keep := applyRules(alert.Labels, current.rules)
			if !keep {
				continue
			}
			alert.Labels = relabeled
			if err := alert.Validate(); err != nil {
				return fmt.Errorf("alert %d after relabeling: %w", i, err)
			}
		}
		kept = append(kept, alert)
	}
	if len(kept) == 0 {
		return nil
	}
	return current.fanout.Send(ctx, kept)
}

func applyRules(input model.LabelSet, rules []*promrelabel.Config) (model.LabelSet, bool) {
	// Prometheus expects a sorted label set. In particular, labelmap must
	// see the same order on firing and resolved updates to the same alert.
	scratch := labels.NewScratchBuilder(len(input))
	for name, value := range input {
		scratch.Add(string(name), string(value))
	}
	scratch.Sort()
	builder := labels.NewBuilder(scratch.Labels())
	if !promrelabel.ProcessBuilder(builder, rules...) {
		return nil, false
	}
	result := make(model.LabelSet, len(input))
	builder.Range(func(l labels.Label) {
		result[model.LabelName(l.Name)] = model.LabelValue(l.Value)
	})
	return result, true
}
