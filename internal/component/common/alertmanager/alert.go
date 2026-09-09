// Package alertmanager contains the typed values used by Alloy alert pipelines.
package alertmanager

import (
	"fmt"

	"github.com/prometheus/common/model"
)

// Alert is the value passed between alert pipeline components.
//
// model.Alert is the Prometheus ecosystem's generic alert representation. State
// is carried separately because an Alertmanager webhook explicitly reports it,
// while model.Alert otherwise infers state from EndsAt and the current time.
type Alert struct {
	model.Alert
	State             model.AlertStatus
	SourceFingerprint string
}

// Validate verifies that an alert can be passed through an alert pipeline.
func (a Alert) Validate() error {
	if a.State != model.AlertFiring && a.State != model.AlertResolved {
		return fmt.Errorf("alert state must be %q or %q", model.AlertFiring, model.AlertResolved)
	}
	if err := a.Alert.Validate(); err != nil {
		return err
	}
	return nil
}

// Clone returns a deep copy of a.
func (a Alert) Clone() Alert {
	a.Labels = a.Labels.Clone()
	a.Annotations = a.Annotations.Clone()
	return a
}

// Fingerprint returns the identity derived from the alert labels.
func (a Alert) Fingerprint() model.Fingerprint {
	return a.Alert.Fingerprint()
}

// CloneAlerts returns a deep copy of alerts.
func CloneAlerts(alerts []Alert) []Alert {
	cloned := make([]Alert, len(alerts))
	for i := range alerts {
		cloned[i] = alerts[i].Clone()
	}
	return cloned
}
