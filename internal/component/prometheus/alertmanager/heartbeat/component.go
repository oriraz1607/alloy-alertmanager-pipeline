// Package heartbeat observes source evaluation timestamps on decoded Alertmanager alerts.
package heartbeat

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"strings"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"

	"github.com/grafana/alloy/internal/component"
	alertpipeline "github.com/grafana/alloy/internal/component/common/alertmanager"
)

const (
	metricName    = "prometheus_alertmanager_heartbeat_last_source_timestamp_seconds"
	maxFutureSkew = 30 * time.Second
)

var lastSourceTimestamp = prometheus.NewDesc(metricName,
	"Newest valid source evaluation time in Unix seconds, or zero until the first firing heartbeat.",
	[]string{"source_network"}, nil)

func init() {
	component.Register(component.Registration{
		Name:      "prometheus.alertmanager.heartbeat",
		Community: true,
		Args:      Arguments{},
		Exports:   Exports{},
		Build: func(opts component.Options, args component.Arguments) (component.Component, error) {
			return New(opts, args.(Arguments))
		},
	})
}

// Arguments configures the networks expected to send PipelineHeartbeat alerts.
type Arguments struct {
	ExpectedNetworks []string `alloy:"expected_networks,attr"`
}

// Validate checks that the expected networks form an explicit, nonempty set.
func (args Arguments) Validate() error {
	if len(args.ExpectedNetworks) == 0 {
		return fmt.Errorf("expected_networks must contain at least one network")
	}
	seen := make(map[string]bool, len(args.ExpectedNetworks))
	for _, network := range args.ExpectedNetworks {
		if strings.TrimSpace(network) == "" || !model.LabelValue(network).IsValid() {
			return fmt.Errorf("expected_networks contains a blank or invalid network name")
		}
		if seen[network] {
			return fmt.Errorf("expected_networks contains duplicate network %q", network)
		}
		seen[network] = true
	}
	return nil
}

// Exports holds a terminal typed-alert receiver.
type Exports struct {
	Receiver alertpipeline.Receiver `alloy:"receiver,attr"`
}

// Component tracks at most one source timestamp per configured network.
type Component struct {
	id  string
	now func() time.Time

	mu     sync.RWMutex
	newest map[string]float64
}

// New creates a prometheus.alertmanager.heartbeat component.
func New(opts component.Options, args Arguments) (*Component, error) {
	return newWithClock(opts, args, time.Now)
}

func newWithClock(opts component.Options, args Arguments, now func() time.Time) (*Component, error) {
	c := &Component{id: opts.ID, now: now}
	if err := c.Update(args); err != nil {
		return nil, err
	}
	if err := opts.Registerer.Register(c); err != nil {
		return nil, fmt.Errorf("registering heartbeat metrics: %w", err)
	}
	opts.OnStateChange(Exports{Receiver: c})
	return c, nil
}

func (c *Component) String() string { return c.id + ".receiver" }

// Run waits for shutdown. Observation and collection are both synchronous.
func (c *Component) Run(ctx context.Context) error {
	<-ctx.Done()
	return nil
}

// Update retains evidence for networks present in both configurations.
func (c *Component) Update(args component.Arguments) error {
	cfg := args.(Arguments)
	if err := cfg.Validate(); err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	next := make(map[string]float64, len(cfg.ExpectedNetworks))
	for _, network := range cfg.ExpectedNetworks {
		next[network] = c.newest[network]
	}
	c.newest = next
	return nil
}

// Send observes matching firing alerts. An unusable probe leaves state unchanged
// and succeeds so it cannot cause retries on sibling fan-out branches.
func (c *Component) Send(ctx context.Context, alerts []alertpipeline.Alert) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	limit := c.now().Add(maxFutureSkew)
	latest := float64(limit.Unix()) + float64(limit.Nanosecond())/1e9
	for _, alert := range alerts {
		if alert.State != model.AlertFiring || alert.Labels[model.AlertNameLabel] != "PipelineHeartbeat" {
			continue
		}
		network := string(alert.Labels["source_network"])
		previous, expected := c.newest[network]
		if !expected {
			continue
		}
		// Prometheus may render the evaluated value in scientific notation.
		// A JSON number accepts decimal and exponent forms and excludes NaN/Inf.
		var source float64
		raw := alert.Annotations["source_timestamp_seconds"]
		if json.Unmarshal([]byte(raw), &source) == nil && source > previous && source <= latest {
			c.newest[network] = source
		}
	}
	return nil
}

// Describe implements prometheus.Collector.
func (c *Component) Describe(ch chan<- *prometheus.Desc) { ch <- lastSourceTimestamp }

// Collect exports a consistent snapshot of exactly the configured network set.
func (c *Component) Collect(ch chan<- prometheus.Metric) {
	c.mu.RLock()
	snapshot := maps.Clone(c.newest)
	c.mu.RUnlock()
	for network, source := range snapshot {
		ch <- prometheus.MustNewConstMetric(lastSourceTimestamp, prometheus.GaugeValue, source, network)
	}
}
