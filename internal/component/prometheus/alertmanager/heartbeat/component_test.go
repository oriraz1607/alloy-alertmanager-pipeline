package heartbeat

import (
	"context"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
	"github.com/stretchr/testify/require"

	"github.com/grafana/alloy/internal/component"
	alertpipeline "github.com/grafana/alloy/internal/component/common/alertmanager"
	"github.com/grafana/alloy/syntax"
)

func TestConfiguration(t *testing.T) {
	registration, ok := component.Get("prometheus.alertmanager.heartbeat")
	require.True(t, ok)
	require.True(t, registration.Community)

	for _, config := range []string{
		``,
		`expected_networks = []`,
		`expected_networks = [""]`,
		`expected_networks = ["A", "A"]`,
	} {
		var args Arguments
		require.Error(t, syntax.Unmarshal([]byte(config), &args), config)
	}
	var args Arguments
	require.NoError(t, syntax.Unmarshal([]byte(`expected_networks = ["A", "B"]`), &args))
	require.Equal(t, []string{"A", "B"}, args.ExpectedNetworks)
}

func TestNewestSourceEvaluation(t *testing.T) {
	now := time.Unix(10_000, 500_000_000)
	c, reg := newTestObserver(t, func() time.Time { return now }, "A", "B", "never")
	require.Equal(t, map[string]float64{"A": 0, "B": 0, "never": 0}, scrape(t, reg))

	firing := probe("A", "9999.5")
	firing.StartsAt = time.Unix(1, 0)
	require.NoError(t, c.Send(t.Context(), []alertpipeline.Alert{firing, probe("B", "1e4")}))
	require.Equal(t, map[string]float64{"A": 9999.5, "B": 10000, "never": 0}, scrape(t, reg))

	now = now.Add(20 * time.Minute)
	// Retry the original notification, deliver an earlier one after it, then
	// deliver a probe which is newer than the previous evidence but still stale.
	require.NoError(t, c.Send(t.Context(), []alertpipeline.Alert{firing, probe("A", "9998")}))
	require.Equal(t, float64(9999.5), scrape(t, reg)["A"])
	require.NoError(t, c.Send(t.Context(), []alertpipeline.Alert{probe("A", "10000"), probe("B", "11200.5")}))
	require.Equal(t, map[string]float64{"A": 10000, "B": 11200.5, "never": 0}, scrape(t, reg))
	require.NoError(t, c.Send(t.Context(), []alertpipeline.Alert{probe("A", "11200.5")}))
	require.Equal(t, float64(11200.5), scrape(t, reg)["A"])
}

func TestIgnoreUnusableProbes(t *testing.T) {
	now := time.Unix(500, 250_000_000)
	c, reg := newTestObserver(t, func() time.Time { return now }, "A", "B")
	resolved := probe("A", "500")
	resolved.State = model.AlertResolved
	production := probe("A", "500")
	production.Labels["alertname"] = "DatabaseDown"

	batch := []alertpipeline.Alert{resolved, production, probe("unexpected", "500"), probe("", "500")}
	for _, timestamp := range []string{"", "abc", "NaN", "+Inf", "-1", "0", "1e999", "530.251"} {
		batch = append(batch, probe("A", timestamp))
	}
	batch = append(batch, probe("B", "530.250"))
	require.NoError(t, c.Send(t.Context(), batch))
	require.Equal(t, map[string]float64{"A": 0, "B": 530.25}, scrape(t, reg))

	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	require.ErrorIs(t, c.Send(ctx, []alertpipeline.Alert{probe("A", "500")}), context.Canceled)
	require.Equal(t, float64(0), scrape(t, reg)["A"])
}

func TestReloadAndRestart(t *testing.T) {
	now := func() time.Time { return time.Unix(300, 0) }
	c, reg := newTestObserver(t, now, "A", "B")
	require.NoError(t, c.Send(t.Context(), []alertpipeline.Alert{probe("A", "294"), probe("B", "298")}))
	require.Error(t, c.Update(Arguments{ExpectedNetworks: []string{"B", "B"}}))
	require.NoError(t, c.Send(t.Context(), []alertpipeline.Alert{probe("A", "299")}))
	require.Equal(t, map[string]float64{"A": 299, "B": 298}, scrape(t, reg))

	require.NoError(t, c.Update(Arguments{ExpectedNetworks: []string{"B", "C"}}))
	require.Equal(t, map[string]float64{"B": 298, "C": 0}, scrape(t, reg))
	require.NoError(t, c.Send(t.Context(), []alertpipeline.Alert{probe("A", "300"), probe("C", "297")}))
	require.Equal(t, map[string]float64{"B": 298, "C": 297}, scrape(t, reg))

	_, restarted := newTestObserver(t, now, "B", "C")
	require.Equal(t, map[string]float64{"B": 0, "C": 0}, scrape(t, restarted))
}

func TestConcurrentSendsKeepMaximum(t *testing.T) {
	c, reg := newTestObserver(t, func() time.Time { return time.Unix(100, 0) }, "A")
	var wg sync.WaitGroup
	for i := 1; i <= 100; i++ {
		wg.Go(func() {
			if err := c.Send(t.Context(), []alertpipeline.Alert{probe("A", strconv.Itoa(i))}); err != nil {
				t.Error(err)
			}
		})
	}
	wg.Wait()
	require.Equal(t, map[string]float64{"A": 100}, scrape(t, reg))
}

func newTestObserver(t *testing.T, clock func() time.Time, networks ...string) (*Component, *prometheus.Registry) {
	t.Helper()
	reg := prometheus.NewPedanticRegistry()
	var exposed alertpipeline.Receiver
	c, err := newWithClock(component.Options{
		ID:            "prometheus.alertmanager.heartbeat.central",
		Registerer:    reg,
		OnStateChange: func(e component.Exports) { exposed = e.(Exports).Receiver },
	}, Arguments{ExpectedNetworks: networks}, clock)
	require.NoError(t, err)
	require.Same(t, c, exposed)
	require.Equal(t, "prometheus.alertmanager.heartbeat.central.receiver", c.String())
	return c, reg
}

func probe(network, timestamp string) alertpipeline.Alert {
	return alertpipeline.Alert{
		State: model.AlertFiring,
		Alert: model.Alert{
			Labels:      model.LabelSet{"alertname": "PipelineHeartbeat", "source_network": model.LabelValue(network)},
			Annotations: model.LabelSet{"source_timestamp_seconds": model.LabelValue(timestamp)},
		},
	}
}

func scrape(t *testing.T, reg *prometheus.Registry) map[string]float64 {
	t.Helper()
	families, err := reg.Gather()
	require.NoError(t, err)
	require.Len(t, families, 1)
	family := families[0]
	require.Equal(t, metricName, family.GetName())
	values := make(map[string]float64)
	for _, series := range family.GetMetric() {
		require.Len(t, series.Label, 1)
		require.Equal(t, "source_network", series.Label[0].GetName())
		values[series.Label[0].GetValue()] = series.GetGauge().GetValue()
	}
	return values
}
