package alertmanager_test

import (
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

// TestLiveSourceHeartbeat runs the documented source rule and notification
// route with real binaries when both are on PATH. Shortened intervals allow
// two successive source evaluations to traverse the whole path in seconds.
func TestLiveSourceHeartbeat(t *testing.T) {
	prometheusBin, err := exec.LookPath("prometheus")
	if err != nil {
		t.Skip("prometheus is not on PATH")
	}
	alertmanagerBin, err := exec.LookPath("alertmanager")
	if err != nil {
		t.Skip("alertmanager is not on PATH")
	}

	dest := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		t.Error("heartbeat reached the incident receiver")
	}))
	t.Cleanup(dest.Close)
	central, metrics := startExample(t, "central.alloy", "5002", "http://alertmanager:9093", dest.URL)
	edge, _ := startExample(t, "edge.alloy", "9095", "http://central-alloy:5002/alerts", central+"/alerts")

	dir := t.TempDir()
	example := func(name string) string {
		data, err := os.ReadFile(filepath.Join("..", "..", "..", "examples", "alertmanager-heartbeat", name))
		require.NoError(t, err)
		return string(data)
	}
	var source struct {
		Spec yaml.Node `yaml:"spec"`
	}
	rule := strings.ReplaceAll(example("source-prometheus-rule.yaml"), "interval: 30s", "interval: 1s")
	require.NoError(t, yaml.Unmarshal([]byte(rule), &source))
	groups, err := yaml.Marshal(&source.Spec)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(dir, "rules.yaml"), groups, 0o600))

	amConfig := strings.NewReplacer("http://edge-alloy:9095", edge,
		"group_wait: 5s", "group_wait: 100ms", "group_interval: 30s", "group_interval: 1s",
		"repeat_interval: 1m", "repeat_interval: 1s").Replace(example("source-alertmanager.yaml"))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "am.yaml"), []byte(amConfig), 0o600))
	ln, err := net.Listen("tcp4", "127.0.0.1:0")
	require.NoError(t, err)
	am := ln.Addr().String()
	require.NoError(t, ln.Close())

	promConfig := fmt.Sprintf("rule_files: [rules.yaml]\nalerting:\n  alertmanagers:\n    - static_configs:\n        - targets: [%q]\n", am)
	require.NoError(t, os.WriteFile(filepath.Join(dir, "prom.yaml"), []byte(promConfig), 0o600))
	start := time.Now()
	runSourceProcess(t, dir, alertmanagerBin, "--config.file=am.yaml", "--storage.path=am-data",
		"--cluster.listen-address=", "--web.listen-address="+am)
	runSourceProcess(t, dir, prometheusBin, "--config.file=prom.yaml", "--storage.tsdb.path=prom-data",
		"--rules.alert.resend-delay=1s", "--web.listen-address=127.0.0.1:0")

	nextSourceTime := func(previous float64) float64 {
		var newest float64
		require.EventuallyWithT(t, func(c *assert.CollectT) {
			for _, m := range metricFamily(c, metrics, "prometheus_alertmanager_heartbeat_last_source_timestamp_seconds") {
				for _, label := range m.Label {
					if label.GetName() == "source_network" && label.GetValue() == "network-a" {
						newest = m.GetGauge().GetValue()
					}
				}
			}
			assert.Greater(c, newest, previous)
		}, 45*time.Second, 100*time.Millisecond)
		return newest
	}
	sourceStartsAt := func() time.Time {
		req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "http://"+am+"/api/v2/alerts", nil)
		require.NoError(t, err)
		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		defer resp.Body.Close()
		require.Equal(t, http.StatusOK, resp.StatusCode)
		var alerts []struct{ StartsAt time.Time }
		require.NoError(t, json.NewDecoder(resp.Body).Decode(&alerts))
		require.Len(t, alerts, 1)
		require.False(t, alerts[0].StartsAt.IsZero())
		return alerts[0].StartsAt
	}

	first := nextSourceTime(float64(start.Unix()) - 1)
	began := sourceStartsAt()
	nextSourceTime(first)
	require.Equal(t, began, sourceStartsAt())
	accepted := metricFamily(t, metrics, "prometheus_alertmanager_write_received_alerts_total")
	require.Len(t, accepted, 1)
	require.Zero(t, accepted[0].GetCounter().GetValue())
}

func runSourceProcess(t *testing.T, dir, binary string, args ...string) {
	t.Helper()
	cmd := exec.CommandContext(t.Context(), binary, append(args, "--log.level=error")...)
	cmd.Dir = dir
	cmd.Stdout, cmd.Stderr = t.Output(), t.Output()
	require.NoError(t, cmd.Start())
	// testing cancels t.Context before running cleanup callbacks.
	t.Cleanup(func() { _ = cmd.Wait() })
}
