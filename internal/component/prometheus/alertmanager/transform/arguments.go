package transform

import "fmt"

// Arguments configures prometheus.alertmanager.transform.
type Arguments struct {
	Template string `alloy:"template,attr"`
	Compact  bool   `alloy:"compact,attr,optional"`
}

// Validate implements syntax.Validator.
func (args *Arguments) Validate() error {
	if args.Template == "" {
		return fmt.Errorf("template must not be empty")
	}
	return nil
}
