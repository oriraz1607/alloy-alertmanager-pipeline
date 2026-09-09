package receive

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/alecthomas/units"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/trace/noop"

	"github.com/grafana/alloy/internal/component"
	alertpipeline "github.com/grafana/alloy/internal/component/common/alertmanager"
	"github.com/grafana/alloy/internal/util"
	"github.com/grafana/alloy/syntax"
)

const singleAlertWebhook = `{
  "receiver": "alloy",
  "status": "firing",
  "alerts": [{
    "status": "firing",
    "labels": {"alertname": "HighCPU", "instance": "server01"},
    "annotations": {"summary": "CPU usage is high"},
    "startsAt": "2026-08-25T08:00:00Z",
    "endsAt": "0001-01-01T00:00:00Z",
    "generatorURL": "http://prometheus/graph",
    "fingerprint": "source-fingerprint"
  }],
  "groupLabels": {},
  "commonLabels": {},
  "commonAnnotations": {},
  "externalURL": "",
  "version": "4",
  "groupKey": ""
}`

func TestRegistrationAndConfiguration(t *testing.T) {
	registration, found := component.Get("prometheus.alertmanager.receive")
	require.True(t, found)
	require.True(t, registration.Community)

	var args Arguments
	require.NoError(t, syntax.Unmarshal([]byte(`
      webhook_path          = "/alerts"
      max_request_body_size = "2MiB"
      forward_timeout       = "15s"
      forward_to            = []

      http {
        listen_address = "0.0.0.0"
        listen_port    = 9095
        tls {
          cert_file = "/tmp/server.crt"
          key_file  = "/tmp/server.key"
        }
      }
    `), &args))
	require.Equal(t, "/alerts", args.WebhookPath)
	require.Equal(t, 2*units.MiB, args.MaxRequestBodySize)
	require.Equal(t, 15*time.Second, args.ForwardTimeout)
	require.Equal(t, "0.0.0.0", args.Server.HTTP.ListenAddress)
	require.Equal(t, 9095, args.Server.HTTP.ListenPort)
	require.Equal(t, "/tmp/server.crt", args.Server.HTTP.TLSConfig.CertFile)
}

func TestValidWebhookForwardsTypedAlert(t *testing.T) {
	var got []alertpipeline.Alert
	c := newHandlerTestComponent(t, alertpipeline.ReceiverFunc(func(_ context.Context, alerts []alertpipeline.Alert) error {
		got = alerts
		return nil
	}))

	response := serveWebhook(c, http.MethodPost, singleAlertWebhook)
	require.Equal(t, http.StatusOK, response.Code)
	require.Len(t, got, 1)
	require.Equal(t, model.AlertFiring, got[0].State)
	require.Equal(t, model.LabelValue("HighCPU"), got[0].Labels["alertname"])
	require.Equal(t, model.LabelValue("CPU usage is high"), got[0].Annotations["summary"])
	require.Equal(t, "http://prometheus/graph", got[0].GeneratorURL)
	require.Equal(t, "source-fingerprint", got[0].SourceFingerprint)
	require.Equal(t, time.Date(2026, 8, 25, 8, 0, 0, 0, time.UTC), got[0].StartsAt)
}

func TestGroupedWebhookPreservesFiringAndResolvedState(t *testing.T) {
	payload := `{
      "alerts": [
        {"status":"firing","labels":{"alertname":"One"},"annotations":{},"startsAt":"2026-08-25T08:00:00Z","endsAt":"0001-01-01T00:00:00Z","generatorURL":""},
        {"status":"resolved","labels":{"alertname":"Two"},"annotations":{"summary":"done"},"startsAt":"2026-08-25T08:01:00Z","endsAt":"2026-08-25T08:02:00Z","generatorURL":""}
      ],
      "version": "4"
    }`
	var got []alertpipeline.Alert
	c := newHandlerTestComponent(t, alertpipeline.ReceiverFunc(func(_ context.Context, alerts []alertpipeline.Alert) error {
		got = alerts
		return nil
	}))
	require.Equal(t, http.StatusOK, serveWebhook(c, http.MethodPost, payload).Code)
	require.Len(t, got, 2)
	require.Equal(t, model.AlertFiring, got[0].State)
	require.Equal(t, model.AlertResolved, got[1].State)
}

func TestMultipleDownstreamReceivers(t *testing.T) {
	var mut sync.Mutex
	counts := make([]int, 2)
	receivers := make([]alertpipeline.Receiver, 2)
	for i := range receivers {
		index := i
		receivers[i] = alertpipeline.ReceiverFunc(func(_ context.Context, alerts []alertpipeline.Alert) error {
			mut.Lock()
			defer mut.Unlock()
			counts[index] += len(alerts)
			return nil
		})
	}
	c := newHandlerTestComponent(t, receivers...)
	require.Equal(t, http.StatusOK, serveWebhook(c, http.MethodPost, singleAlertWebhook).Code)
	require.Equal(t, []int{1, 1}, counts)
}

func TestRejectedIncomingRequests(t *testing.T) {
	tests := []struct {
		name       string
		method     string
		body       string
		maxBody    int64
		wantStatus int
	}{
		{name: "method", method: http.MethodGet, body: singleAlertWebhook, wantStatus: http.StatusMethodNotAllowed},
		{name: "malformed JSON", method: http.MethodPost, body: `{"alerts":[`, wantStatus: http.StatusBadRequest},
		{name: "multiple JSON values", method: http.MethodPost, body: `{"alerts":[]} {}`, wantStatus: http.StatusBadRequest},
		{name: "empty alerts", method: http.MethodPost, body: `{"alerts":[]}`, wantStatus: http.StatusBadRequest},
		{name: "missing labels", method: http.MethodPost, body: `{"alerts":[{"status":"firing","labels":{},"annotations":{}}]}`, wantStatus: http.StatusBadRequest},
		{name: "missing status", method: http.MethodPost, body: `{"alerts":[{"labels":{"alertname":"Example"},"annotations":{}}]}`, wantStatus: http.StatusBadRequest},
		{name: "unknown status", method: http.MethodPost, body: `{"alerts":[{"status":"pending","labels":{"alertname":"Example"},"annotations":{}}]}`, wantStatus: http.StatusBadRequest},
		{name: "body too large", method: http.MethodPost, body: singleAlertWebhook, maxBody: int64(len(singleAlertWebhook) - 1), wantStatus: http.StatusRequestEntityTooLarge},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var calls int
			c := newHandlerTestComponent(t, alertpipeline.ReceiverFunc(func(context.Context, []alertpipeline.Alert) error {
				calls++
				return nil
			}))
			if tc.maxBody > 0 {
				c.state.maxRequestBodySize = tc.maxBody
			}
			response := serveWebhook(c, tc.method, tc.body)
			require.Equal(t, tc.wantStatus, response.Code)
			require.Zero(t, calls)
		})
	}
}

func TestDownstreamFailureResponses(t *testing.T) {
	t.Run("rejection", func(t *testing.T) {
		c := newHandlerTestComponent(t, alertpipeline.ReceiverFunc(func(context.Context, []alertpipeline.Alert) error {
			return errors.New("queue full")
		}))
		require.Equal(t, http.StatusServiceUnavailable, serveWebhook(c, http.MethodPost, singleAlertWebhook).Code)
	})

	t.Run("timeout", func(t *testing.T) {
		c := newHandlerTestComponent(t, alertpipeline.ReceiverFunc(func(ctx context.Context, _ []alertpipeline.Alert) error {
			<-ctx.Done()
			return ctx.Err()
		}))
		c.state.forwardTimeout = time.Millisecond
		require.Equal(t, http.StatusGatewayTimeout, serveWebhook(c, http.MethodPost, singleAlertWebhook).Code)
	})
}

func TestShutdownStopsServer(t *testing.T) {
	var args Arguments
	args.SetToDefault()
	args.Server.HTTP.ListenPort = 0
	c, err := New(component.Options{
		ID:         "prometheus.alertmanager.receive.shutdown",
		Logger:     util.TestLogger(t),
		Registerer: prometheus.NewRegistry(),
		Tracer:     noop.NewTracerProvider(),
	}, args)
	require.NoError(t, err)
	require.Equal(t, component.HealthTypeHealthy, c.CurrentHealth().Health)

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- c.Run(ctx) }()
	cancel()
	require.NoError(t, <-done)
	require.Equal(t, component.HealthTypeUnknown, c.CurrentHealth().Health)
}

func newHandlerTestComponent(t *testing.T, receivers ...alertpipeline.Receiver) *Component {
	t.Helper()
	registry := prometheus.NewRegistry()
	return &Component{
		opts:    component.Options{ID: "receive.test", Logger: util.TestLogger(t), Registerer: registry},
		metrics: newMetrics(registry),
		fanout:  alertpipeline.NewFanout(receivers),
		state: handlerState{
			maxRequestBodySize: int64(units.MiB),
			forwardTimeout:     time.Second,
		},
	}
}

func serveWebhook(c *Component, method, body string) *httptest.ResponseRecorder {
	request := httptest.NewRequest(method, "/webhook", bytes.NewBufferString(body))
	response := httptest.NewRecorder()
	c.handleWebhook(response, request)
	return response
}
