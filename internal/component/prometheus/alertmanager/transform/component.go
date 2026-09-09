package transform

import (
	"context"
	"fmt"

	"github.com/grafana/alloy/internal/component"
	alertpipeline "github.com/grafana/alloy/internal/component/common/alertmanager"
)

func init() {
	component.Register(component.Registration{
		Name:      "prometheus.alertmanager.transform",
		Community: true,
		Args:      Arguments{},
		Exports:   Exports{},
		Build: func(opts component.Options, args component.Arguments) (component.Component, error) {
			return New(opts, args.(Arguments))
		},
	})
}

// Exports holds the configured alert transformer.
type Exports struct {
	Transformer alertpipeline.Transformer `alloy:"transformer,attr"`
}

// Component owns a reloadable JSON template transformer.
type Component struct {
	opts        component.Options
	transformer *templateTransformer
}

var (
	_ component.Component     = (*Component)(nil)
	_ component.LiveDebugging = (*Component)(nil)
)

// New creates a prometheus.alertmanager.transform component.
func New(opts component.Options, args Arguments) (*Component, error) {
	transformer := &templateTransformer{id: opts.ID, debugDataPublisher: alertpipeline.NewDebugPublisher(opts), logger: opts.Logger}
	c := &Component{opts: opts, transformer: transformer}
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

// Update compiles and atomically installs a new template.
func (c *Component) Update(args component.Arguments) error {
	newArgs := args.(Arguments)
	if err := newArgs.Validate(); err != nil {
		return err
	}
	tmpl, err := compileTemplate(newArgs.Template)
	if err != nil {
		return fmt.Errorf("compiling alert JSON template: %w", err)
	}
	c.transformer.update(tmpl, newArgs.Compact)
	c.opts.OnStateChange(Exports{Transformer: c.transformer})
	return nil
}

func (c *Component) LiveDebugging() {}
