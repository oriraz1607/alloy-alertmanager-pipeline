package decode

import (
	"context"
	"fmt"

	"github.com/grafana/alloy/internal/component"
	alertpipeline "github.com/grafana/alloy/internal/component/common/alertmanager"
)

func init() {
	component.Register(component.Registration{
		Name:      "prometheus.alertmanager.decode",
		Community: true,
		Args:      Arguments{},
		Exports:   Exports{},
		Build: func(opts component.Options, args component.Arguments) (component.Component, error) {
			return New(opts, args.(Arguments))
		},
	})
}

// Exports holds the configured alert decoder.
type Exports struct {
	Decoder alertpipeline.Decoder `alloy:"decoder,attr"`
}

// Component owns a reloadable JSON field-mapping decoder.
type Component struct {
	opts    component.Options
	decoder *mappingDecoder
}

var (
	_ component.Component     = (*Component)(nil)
	_ component.LiveDebugging = (*Component)(nil)
)

// New creates a prometheus.alertmanager.decode component.
func New(opts component.Options, args Arguments) (*Component, error) {
	c := &Component{opts: opts, decoder: &mappingDecoder{id: opts.ID, debugDataPublisher: alertpipeline.NewDebugPublisher(opts)}}
	if err := c.Update(args); err != nil {
		return nil, err
	}
	return c, nil
}

// Run waits until Alloy stops the component.
func (c *Component) Run(ctx context.Context) error {
	<-ctx.Done()
	return nil
}

// Update validates and atomically installs a new mapping.
func (c *Component) Update(args component.Arguments) error {
	newArgs := args.(Arguments)
	if err := newArgs.Validate(); err != nil {
		return err
	}
	mapping, err := compileMapping(newArgs)
	if err != nil {
		return fmt.Errorf("compiling alert JSON mapping: %w", err)
	}
	c.decoder.update(mapping)
	c.opts.OnStateChange(Exports{Decoder: c.decoder})
	return nil
}

func (c *Component) LiveDebugging() {}
