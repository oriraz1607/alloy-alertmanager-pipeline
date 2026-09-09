package receive

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/trace/noop"

	"github.com/grafana/alloy/internal/component"
	alertpipeline "github.com/grafana/alloy/internal/component/common/alertmanager"
	alertwrite "github.com/grafana/alloy/internal/component/prometheus/alertmanager/write"
	"github.com/grafana/alloy/internal/util"
)

func TestWebhookToAlertmanagerWritePipeline(t *testing.T) {
	var mut sync.Mutex
	var received []map[string]any
	destination := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var alerts []map[string]any
		require.NoError(t, json.NewDecoder(r.Body).Decode(&alerts))
		mut.Lock()
		received = append(received, alerts...)
		mut.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	defer destination.Close()

	var writeArgs alertwrite.Arguments
	writeArgs.SetToDefault()
	writeArgs.Endpoint.URL = destination.URL
	writeArgs.Endpoint.BatchSize = 2
	writeArgs.Endpoint.BatchWait = time.Millisecond
	writeArgs.Endpoint.MinBackoff = time.Millisecond
	writeArgs.Endpoint.MaxBackoff = time.Millisecond
	writeArgs.RefreshInterval = time.Hour
	writeArgs.FiringAlertDuration = 2 * time.Hour
	writeArgs.FiringAlertTimeout = 3 * time.Hour
	writeArgs.ResolvedRetention = 0

	var writeReceiver alertpipeline.Receiver
	writer, err := alertwrite.New(component.Options{
		ID:         "prometheus.alertmanager.write.integration",
		Logger:     util.TestLogger(t),
		Registerer: prometheus.NewRegistry(),
		Tracer:     noop.NewTracerProvider(),
		OnStateChange: func(exports component.Exports) {
			writeReceiver = exports.(alertwrite.Exports).Receiver
		},
	}, writeArgs)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- writer.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		require.NoError(t, <-done)
	})

	receiver := newHandlerTestComponent(t, writeReceiver)
	payload := `{
      "alerts": [
        {"status":"firing","labels":{"alertname":"One","instance":"edge"},"annotations":{"summary":"still firing"},"startsAt":"2026-08-25T08:00:00Z","endsAt":"0001-01-01T00:00:00Z","generatorURL":"http://prometheus/one"},
        {"status":"resolved","labels":{"alertname":"Two","instance":"edge"},"annotations":{"summary":"resolved"},"startsAt":"2026-08-25T08:01:00Z","endsAt":"2026-08-25T08:02:00Z","generatorURL":"http://prometheus/two"}
      ],
      "version":"4"
    }`
	require.Equal(t, http.StatusOK, serveWebhook(receiver, http.MethodPost, payload).Code)
	require.Eventually(t, func() bool {
		mut.Lock()
		defer mut.Unlock()
		return len(received) == 2
	}, time.Second, 5*time.Millisecond)

	mut.Lock()
	defer mut.Unlock()
	require.Equal(t, "One", received[0]["labels"].(map[string]any)["alertname"])
	require.Equal(t, "edge", received[0]["labels"].(map[string]any)["instance"])
	require.Equal(t, "still firing", received[0]["annotations"].(map[string]any)["summary"])
	require.Equal(t, "2026-08-25T08:00:00.000Z", received[0]["startsAt"])
	require.Equal(t, "http://prometheus/one", received[0]["generatorURL"])
	require.Equal(t, "Two", received[1]["labels"].(map[string]any)["alertname"])
	require.Equal(t, "resolved", received[1]["annotations"].(map[string]any)["summary"])
	require.Equal(t, "2026-08-25T08:02:00.000Z", received[1]["endsAt"])
}
