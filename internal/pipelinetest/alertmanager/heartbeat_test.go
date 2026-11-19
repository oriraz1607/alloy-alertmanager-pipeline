package alertmanager_test

import (
	"bytes"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/alertmanager/notify/webhook"
	"github.com/prometheus/alertmanager/template"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/alloy/internal/featuregate"
	"github.com/grafana/alloy/internal/runtime"
	"github.com/grafana/alloy/internal/runtime/logging"

	_ "github.com/grafana/alloy/internal/component/prometheus/alertmanager/decode"
	_ "github.com/grafana/alloy/internal/component/prometheus/alertmanager/heartbeat"
	_ "github.com/grafana/alloy/internal/component/prometheus/alertmanager/http"
	_ "github.com/grafana/alloy/internal/component/prometheus/alertmanager/http_receive"
	_ "github.com/grafana/alloy/internal/component/prometheus/alertmanager/receive"
	_ "github.com/grafana/alloy/internal/component/prometheus/alertmanager/relabel"
	_ "github.com/grafana/alloy/internal/component/prometheus/alertmanager/transform"
	_ "github.com/grafana/alloy/internal/component/prometheus/alertmanager/write"
)

// Run the operator's examples through the Alloy runtime, with only deployment
// addresses, ports, and the second edge's network name changed for the test.
func TestNetworkHeartbeatPipeline(t *testing.T) {
	var mu sync.Mutex
	var incidents []model.Alert
	destination := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, http.MethodPost, r.Method)
		assert.Equal(t, "/api/v2/alerts", r.URL.Path)
		var batch []model.Alert
		if err := json.NewDecoder(r.Body).Decode(&batch); err != nil {
			t.Errorf("decoding downstream POST: %v", err)
			http.Error(w, "invalid alerts", http.StatusBadRequest)
			return
		}
		mu.Lock()
		incidents = append(incidents, batch...)
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(destination.Close)

	central, metrics := startExample(t, "central.alloy", "5002", "http://alertmanager:9093", destination.URL)
	edge := func(network string) string {
		addr, _ := startExample(t, "edge.alloy", "9095", "http://central-alloy:5002/alerts", central+"/alerts", "network-a", network)
		return addr + "/webhook"
	}
	a, b := edge("network-a"), edge("network-b")

	// Move source evaluation and monitoring time in lockstep, independently of
	// HTTP worker timing. All source times are in the past for the real observer.
	now := time.Now().UTC().Truncate(time.Second).Add(-21 * time.Minute)
	began := now.Add(-24 * time.Hour)
	probe := func(at time.Time) template.Alert {
		return template.Alert{
			Status:      "firing",
			StartsAt:    began,
			Labels:      template.KV{"alertname": "PipelineHeartbeat"},
			Annotations: template.KV{"source_timestamp_seconds": strconv.FormatInt(at.Unix(), 10)},
		}
	}
	// Wait for the listener's success counter so negative assertions cannot
	// accidentally pass simply because a bad or queued probe is still in flight.
	var submitted int
	send := func(url string, alerts ...template.Alert) {
		body, err := json.Marshal(webhook.Message{Version: "4", Data: &template.Data{Status: "firing", Alerts: alerts}})
		require.NoError(t, err)
		req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, url, bytes.NewReader(body))
		require.NoError(t, err)
		req.Header.Set("Content-Type", "application/json")
		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		require.NoError(t, resp.Body.Close())
		require.Equal(t, http.StatusOK, resp.StatusCode)
		submitted += len(alerts)
	}
	lastSeen := func() map[string]float64 {
		require.EventuallyWithT(t, func(c *assert.CollectT) {
			forwarded := metricFamily(c, metrics, "prometheus_alertmanager_http_receive_forwarded_alerts_total")
			require.Len(c, forwarded, 1)
			assert.Equal(c, float64(submitted), forwarded[0].GetCounter().GetValue())
		}, 5*time.Second, 10*time.Millisecond)
		latest := make(map[string]float64)
		for _, m := range metricFamily(t, metrics, "prometheus_alertmanager_heartbeat_last_source_timestamp_seconds") {
			for _, label := range m.Label {
				if label.GetName() == "source_network" {
					latest[label.GetValue()] = m.GetGauge().GetValue()
				}
			}
		}
		return latest
	}
	stale := func(latest map[string]float64) []string {
		var networks []string
		for network, timestamp := range latest {
			if float64(now.Unix())-timestamp > 5*60 {
				networks = append(networks, network)
			}
		}
		return networks
	}

	require.Equal(t, map[string]float64{"network-a": 0, "network-b": 0}, lastSeen())
	send(a, probe(now))
	send(b, probe(now))
	latest := lastSeen()
	require.Equal(t, map[string]float64{"network-a": float64(now.Unix()), "network-b": float64(now.Unix())}, latest)
	require.Empty(t, stale(latest))

	// B continues to notify each minute while A falls silent.
	for range 21 {
		now = now.Add(time.Minute)
		send(b, probe(now))
	}
	require.Equal(t, []string{"network-a"}, stale(lastSeen()))

	// This queued notification is newer than A's last observation but still
	// twenty minutes old at central evaluation time.
	queued := now.Add(-20 * time.Minute)
	send(a, probe(queued))
	latest = lastSeen()
	require.Equal(t, float64(queued.Unix()), latest["network-a"])
	require.Equal(t, []string{"network-a"}, stale(latest))

	malformed := probe(now)
	malformed.Annotations["source_timestamp_seconds"] = "invalid"
	production := template.Alert{
		Status:      "firing",
		StartsAt:    began,
		Labels:      template.KV{"alertname": "DiskFull", "clustername": "prod"},
		Annotations: template.KV{"summary": "filesystem is full"},
	}
	send(a, malformed, production)
	require.Equal(t, latest, lastSeen())

	send(a, probe(now))
	latest = lastSeen()
	require.Equal(t, float64(now.Unix()), latest["network-a"])
	require.Empty(t, stale(latest))
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		mu.Lock()
		defer mu.Unlock()
		require.Len(c, incidents, 1)
		assert.Equal(c, model.LabelSet{"alertname": "DiskFull", "cluster": "prod", "source_network": "network-a"}, incidents[0].Labels)
		assert.Equal(c, model.LabelValue("filesystem is full"), incidents[0].Annotations["summary"])
	}, 5*time.Second, 10*time.Millisecond)

	// The writer's input counter is synchronous. A leak onto the incident path
	// must fail even if its outgoing POST hasn't run yet.
	received := metricFamily(t, metrics, "prometheus_alertmanager_write_received_alerts_total")
	require.Len(t, received, 1)
	require.Equal(t, float64(1), received[0].GetCounter().GetValue())
}

func metricFamily(t require.TestingT, reg prometheus.Gatherer, name string) []*dto.Metric {
	families, err := reg.Gather()
	require.NoError(t, err)
	for _, f := range families {
		if f.GetName() == name {
			return f.Metric
		}
	}
	require.FailNow(t, "missing metric", "%s", name)
	return nil
}

func startExample(t *testing.T, name, examplePort string, replacements ...string) (string, prometheus.Gatherer) {
	t.Helper()
	path := filepath.Join("..", "..", "..", "examples", "alertmanager-heartbeat", name)
	file, err := os.ReadFile(path)
	require.NoError(t, err)

	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = listener.Close() })
	addr := listener.Addr().(*net.TCPAddr)
	replacements = append(replacements, "0.0.0.0", "127.0.0.1", examplePort, strconv.Itoa(addr.Port))
	for i := 0; i < len(replacements); i += 2 {
		require.Contains(t, string(file), replacements[i], "example deployment parameter changed")
	}
	config := strings.NewReplacer(replacements...).Replace(string(file))
	source, err := runtime.ParseSource(path, []byte(config))
	require.NoError(t, err)
	registry := prometheus.NewRegistry()
	ctrl, err := runtime.New(runtime.Options{
		DataPath: t.TempDir(), Reg: registry, Logger: logging.NewNop(),
		EnableCommunityComps: true, MinStability: featuregate.StabilityGenerallyAvailable,
		TaskShutdownDeadline: 5 * time.Second,
	})
	require.NoError(t, err)
	var wg sync.WaitGroup
	wg.Go(func() { ctrl.Run(t.Context()) })
	t.Cleanup(wg.Wait)
	require.NoError(t, listener.Close())
	require.NoError(t, ctrl.LoadSource(source, nil, path))
	require.Eventually(t, ctrl.LoadComplete, 5*time.Second, 10*time.Millisecond)
	return "http://" + addr.String(), registry
}
