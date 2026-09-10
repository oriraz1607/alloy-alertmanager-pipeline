package transform

import "fmt"

// Arguments configures prometheus.alertmanager.transform.
type Arguments struct {
	Template                string `alloy:"template,attr"`
	RemoveSpecialCharacters bool   `alloy:"remove_special_characters,attr,optional"`
	Compact                 bool   `alloy:"compact,attr,optional"`
}

// Validate implements syntax.Validator.
func (args *Arguments) Validate() error {
	if args.Template == "" {
		return fmt.Errorf("template must not be empty")
	}
	return nil
}
