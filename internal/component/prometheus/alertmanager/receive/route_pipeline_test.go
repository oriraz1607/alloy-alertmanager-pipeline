package receive

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/alertmanager/notify/webhook"
	"github.com/prometheus/alertmanager/template"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"

	"github.com/grafana/alloy/internal/component"
	"github.com/grafana/alloy/internal/featuregate"
	"github.com/grafana/alloy/internal/runtime"
	"github.com/grafana/alloy/internal/runtime/logging"

	// Register the components used in the test pipeline.
	_ "github.com/grafana/alloy/internal/component/prometheus/alertmanager/http"
	_ "github.com/grafana/alloy/internal/component/prometheus/alertmanager/route"
	_ "github.com/grafana/alloy/internal/component/prometheus/alertmanager/transform"
)

func TestWebhookRoutePipeline(t *testing.T) {
	var mu sync.Mutex
	got := make(map[string]template.Alerts)
	destination := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			template.Alert
			SourceFingerprint string
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Errorf("invalid JSON at %s: %v", r.URL.Path, err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		body.Alert.Fingerprint = body.SourceFingerprint
		mu.Lock()
		got[r.URL.Path] = append(got[r.URL.Path], body.Alert)
		mu.Unlock()
	}))
	t.Cleanup(destination.Close)
	t.Setenv("ROUTE_TEST_DESTINATION", destination.URL)

	logger, err := logging.New(io.Discard, logging.DefaultOptions)
	require.NoError(t, err)
	registry := prometheus.NewRegistry()
	ctrl, err := runtime.New(runtime.Options{
		Logger:               logger,
		DataPath:             t.TempDir(),
		Reg:                  registry,
		MinStability:         featuregate.StabilityGenerallyAvailable,
		EnableCommunityComps: true,
	})
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(t.Context())
	var wg sync.WaitGroup
	wg.Go(func() { ctrl.Run(ctx) })
	t.Cleanup(func() {
		cancel()
		wg.Wait()
	})

	config, err := os.ReadFile("testdata/route.alloy")
	require.NoError(t, err)
	source, err := runtime.ParseSource("route.alloy", config)
	require.NoError(t, err)
	require.NoError(t, ctrl.LoadSource(source, nil, ""))
	require.Eventually(t, ctrl.LoadComplete, 5*time.Second, 10*time.Millisecond)
	info, err := ctrl.GetComponent(component.ParseID("prometheus.alertmanager.receive.incoming"), component.InfoOptions{})
	require.NoError(t, err)
	url := "http://" + info.Component.(*Component).server.HTTPListenAddr() + "/webhook"

	want := make(map[string]template.Alerts)
	for _, status := range []string{"firing", "resolved"} {
		alerts := groupedRoutingAlerts(status)
		postRoutingWebhook(t, url, alerts)
		want["/noc"] = append(want["/noc"], alerts[0], alerts[2])
		want["/payments"] = append(want["/payments"], alerts[0], alerts[1])
		want["/general"] = append(want["/general"], alerts[3])
	}

	// Admission is synchronous. Wait for all admitted HTTP requests to finish so
	// an extra queued delivery (for example, C sent to general) cannot escape the assertion.
	waitForRoutingDeliveries(t, registry)
	mu.Lock()
	firstDeliveries := got
	got = make(map[string]template.Alerts)
	mu.Unlock()
	require.Equal(t, want, firstDeliveries)

	// Give the payments sender the NOC URL. A is selected by both named senders
	// and must be delivered twice even though their endpoints are identical.
	sharedConfig := bytes.ReplaceAll(config, []byte("/payments"), []byte("/noc"))
	sharedSource, err := runtime.ParseSource("route.alloy", sharedConfig)
	require.NoError(t, err)
	require.NoError(t, ctrl.LoadSource(sharedSource, nil, ""))
	overlap := groupedRoutingAlerts("firing")[:1]
	postRoutingWebhook(t, url, overlap)
	waitForRoutingDeliveries(t, registry)
	mu.Lock()
	defer mu.Unlock()
	require.Equal(t, map[string]template.Alerts{"/noc": {overlap[0], overlap[0]}}, got)
}

func postRoutingWebhook(t *testing.T, url string, alerts template.Alerts) {
	t.Helper()
	message := webhook.Message{
		Version: "4",
		Data: &template.Data{
			Receiver: "alloy", Status: alerts[0].Status, Alerts: alerts,
			GroupLabels: template.KV{"environment": "prod"}, CommonLabels: template.KV{"environment": "prod"},
		},
	}
	payload, err := json.Marshal(message)
	require.NoError(t, err)
	resp, err := http.Post(url, "application/json", bytes.NewReader(payload))
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	require.Equal(t, http.StatusOK, resp.StatusCode)
}

func waitForRoutingDeliveries(t *testing.T, registry *prometheus.Registry) {
	t.Helper()
	require.Eventually(t, func() bool {
		families, err := registry.Gather()
		if err != nil {
			return false
		}
		var accepted, sent float64
		for _, family := range families {
			for _, sample := range family.Metric {
				switch family.GetName() {
				case "prometheus_alertmanager_http_received_alerts_total":
					accepted += sample.GetCounter().GetValue()
				case "prometheus_alertmanager_http_sent_alerts_total":
					sent += sample.GetCounter().GetValue()
				}
			}
		}
		return accepted > 0 && accepted == sent
	}, 5*time.Second, 10*time.Millisecond)
}

func groupedRoutingAlerts(status string) template.Alerts {
	start := time.Date(2026, 9, 7, 14, 0, 0, 123456789, time.UTC)
	var end time.Time
	if status == "resolved" {
		end = start.Add(time.Hour)
	}
	alert := func(name, severity, namespace string) template.Alert {
		return template.Alert{
			Status:      status,
			Labels:      template.KV{"alertname": name, "severity": severity, "namespace": namespace, "environment": "prod"},
			Annotations: template.KV{"summary": "alert " + name}, StartsAt: start, EndsAt: end,
			GeneratorURL: "https://prometheus.example.com/graph#" + name, Fingerprint: "source-" + name,
		}
	}
	return template.Alerts{
		alert("A", "critical", "payments"),
		alert("B", "warning", "payments"),
		alert("C", "critical", "infra"),
		alert("D", "warning", "infra"),
	}
}
