package http_receive

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
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
)

func TestRegistrationAndConfiguration(t *testing.T) {
	registration, found := component.Get("prometheus.alertmanager.http_receive")
	require.True(t, found)
	require.True(t, registration.Community)

	decoder := alertpipeline.DecoderFunc(func([]byte) (alertpipeline.Alert, error) { return validAlert(), nil })
	receiver := alertpipeline.ReceiverFunc(func(context.Context, []alertpipeline.Alert) error { return nil })
	var args Arguments
	args.SetToDefault()
	args.Path = "/api/custom-alerts"
	args.MaxRequestBodySize = 2 * units.MiB
	args.ForwardTimeout = 15 * time.Second
	args.Server.HTTP.ListenAddress = "0.0.0.0"
	args.Server.HTTP.ListenPort = 9095
	args.Decoder = decoder
	args.ForwardTo = []alertpipeline.Receiver{receiver}
	require.NoError(t, args.Validate())
	require.Equal(t, "/api/custom-alerts", args.Path)
	require.Equal(t, 2*units.MiB, args.MaxRequestBodySize)
}

func TestConfiguredPathMethodDecodeAndForward(t *testing.T) {
	var got []alertpipeline.Alert
	c := newHandlerComponent(t,
		"/alerts",
		alertpipeline.DecoderFunc(func(body []byte) (alertpipeline.Alert, error) {
			require.JSONEq(t, `{"name":"HighCPU"}`, string(body))
			return validAlert(), nil
		}),
		alertpipeline.ReceiverFunc(func(_ context.Context, alerts []alertpipeline.Alert) error {
			got = alerts
			return nil
		}),
	)
	require.Equal(t, http.StatusOK, serve(c, http.MethodPost, "/alerts", `{"name":"HighCPU"}`).Code)
	require.Len(t, got, 1)
	require.Equal(t, model.LabelValue("HighCPU"), got[0].Labels["alertname"])

	got = nil
	require.Equal(t, http.StatusNotFound, serve(c, http.MethodPost, "/webhook", `{}`).Code)
	require.Nil(t, got)
	require.Equal(t, http.StatusMethodNotAllowed, serve(c, http.MethodGet, "/alerts", `{}`).Code)
}

func TestArbitraryConfiguredPaths(t *testing.T) {
	for _, path := range []string{"/alerts", "/webhook", "/api/custom-alerts"} {
		t.Run(path, func(t *testing.T) {
			calls := 0
			c := newHandlerComponent(t, path,
				alertpipeline.DecoderFunc(func([]byte) (alertpipeline.Alert, error) { return validAlert(), nil }),
				alertpipeline.ReceiverFunc(func(context.Context, []alertpipeline.Alert) error { calls++; return nil }),
			)
			require.Equal(t, http.StatusOK, serve(c, http.MethodPost, path, `{}`).Code)
			require.Equal(t, 1, calls)
		})
	}
}

func TestReceiverInstancesUseIndependentPaths(t *testing.T) {
	alertsCalls := 0
	webhookCalls := 0
	decoder := alertpipeline.DecoderFunc(func([]byte) (alertpipeline.Alert, error) { return validAlert(), nil })
	alerts := newHandlerComponent(t, "/alerts", decoder, alertpipeline.ReceiverFunc(func(context.Context, []alertpipeline.Alert) error {
		alertsCalls++
		return nil
	}))
	webhook := newHandlerComponent(t, "/webhook", decoder, alertpipeline.ReceiverFunc(func(context.Context, []alertpipeline.Alert) error {
		webhookCalls++
		return nil
	}))

	require.Equal(t, http.StatusOK, serve(alerts, http.MethodPost, "/alerts", `{}`).Code)
	require.Equal(t, http.StatusNotFound, serve(alerts, http.MethodPost, "/webhook", `{}`).Code)
	require.Equal(t, http.StatusOK, serve(webhook, http.MethodPost, "/webhook", `{}`).Code)
	require.Equal(t, http.StatusNotFound, serve(webhook, http.MethodPost, "/alerts", `{}`).Code)
	require.Equal(t, 1, alertsCalls)
	require.Equal(t, 1, webhookCalls)
}

func TestDecoderAndDownstreamFailuresAreNotAcknowledged(t *testing.T) {
	t.Run("decoder", func(t *testing.T) {
		calls := 0
		c := newHandlerComponent(t, "/alerts",
			alertpipeline.DecoderFunc(func([]byte) (alertpipeline.Alert, error) { return alertpipeline.Alert{}, errors.New("schema mismatch") }),
			alertpipeline.ReceiverFunc(func(context.Context, []alertpipeline.Alert) error { calls++; return nil }),
		)
		require.Equal(t, http.StatusBadRequest, serve(c, http.MethodPost, "/alerts", `{}`).Code)
		require.Zero(t, calls)
	})

	t.Run("downstream rejection", func(t *testing.T) {
		c := newHandlerComponent(t, "/alerts",
			alertpipeline.DecoderFunc(func([]byte) (alertpipeline.Alert, error) { return validAlert(), nil }),
			alertpipeline.ReceiverFunc(func(context.Context, []alertpipeline.Alert) error { return errors.New("queue full") }),
		)
		require.Equal(t, http.StatusServiceUnavailable, serve(c, http.MethodPost, "/alerts", `{}`).Code)
	})

	t.Run("downstream timeout", func(t *testing.T) {
		c := newHandlerComponent(t, "/alerts",
			alertpipeline.DecoderFunc(func([]byte) (alertpipeline.Alert, error) { return validAlert(), nil }),
			alertpipeline.ReceiverFunc(func(ctx context.Context, _ []alertpipeline.Alert) error { <-ctx.Done(); return ctx.Err() }),
		)
		c.state.forwardTimeout = time.Millisecond
		require.Equal(t, http.StatusGatewayTimeout, serve(c, http.MethodPost, "/alerts", `{}`).Code)
	})
}

func TestBodyLimit(t *testing.T) {
	c := newHandlerComponent(t, "/alerts",
		alertpipeline.DecoderFunc(func([]byte) (alertpipeline.Alert, error) { return validAlert(), nil }),
		alertpipeline.ReceiverFunc(func(context.Context, []alertpipeline.Alert) error { return nil }),
	)
	c.state.maxRequestBodySize = 1
	require.Equal(t, http.StatusRequestEntityTooLarge, serve(c, http.MethodPost, "/alerts", `{}`).Code)
}

func TestPathReloadStopsOldRouteAndStartsNewRoute(t *testing.T) {
	accepted := 0
	args := defaultArgs(
		"/alerts",
		alertpipeline.DecoderFunc(func([]byte) (alertpipeline.Alert, error) { return validAlert(), nil }),
		alertpipeline.ReceiverFunc(func(context.Context, []alertpipeline.Alert) error { accepted++; return nil }),
	)
	c, err := New(component.Options{
		ID: "prometheus.alertmanager.http_receive.reload", Logger: util.TestLogger(t), Registerer: prometheus.NewRegistry(), Tracer: noop.NewTracerProvider(),
	}, args)
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- c.Run(ctx) }()
	t.Cleanup(func() { cancel(); require.NoError(t, <-done) })

	args.Path = "/webhook"
	require.NoError(t, c.Update(args))
	baseURL := "http://" + c.server.HTTPListenAddr()
	response, err := http.Post(baseURL+"/alerts", "application/json", bytes.NewBufferString(`{}`))
	require.NoError(t, err)
	require.Equal(t, http.StatusNotFound, response.StatusCode)
	require.NoError(t, response.Body.Close())
	response, err = http.Post(baseURL+"/webhook", "application/json", bytes.NewBufferString(`{}`))
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, response.StatusCode)
	require.NoError(t, response.Body.Close())
	require.Equal(t, 1, accepted)
}

func TestMetricsDoNotCollideWithTargetServer(t *testing.T) {
	registry := prometheus.NewRegistry()
	args := defaultArgs(
		"/alerts",
		alertpipeline.DecoderFunc(func([]byte) (alertpipeline.Alert, error) { return validAlert(), nil }),
		alertpipeline.ReceiverFunc(func(context.Context, []alertpipeline.Alert) error { return nil }),
	)
	c, err := New(component.Options{
		ID: "prometheus.alertmanager.http_receive.metrics", Logger: util.TestLogger(t), Registerer: registry, Tracer: noop.NewTracerProvider(),
	}, args)
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- c.Run(ctx) }()
	t.Cleanup(func() { cancel(); require.NoError(t, <-done) })

	response, err := http.Post("http://"+c.server.HTTPListenAddr()+"/alerts", "application/json", bytes.NewBufferString(`{}`))
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, response.StatusCode)
	require.NoError(t, response.Body.Close())
	_, err = registry.Gather()
	require.NoError(t, err)
}

func TestInvalidPaths(t *testing.T) {
	decoder := alertpipeline.DecoderFunc(func([]byte) (alertpipeline.Alert, error) { return validAlert(), nil })
	receiver := alertpipeline.ReceiverFunc(func(context.Context, []alertpipeline.Alert) error { return nil })
	for _, path := range []string{"", "alerts", "/alerts/", "/a/../alerts", "/alerts?tenant=a", "/alerts#fragment"} {
		args := defaultArgs(path, decoder, receiver)
		require.Error(t, args.Validate(), path)
	}
}

func newHandlerComponent(t *testing.T, path string, decoder alertpipeline.Decoder, receiver alertpipeline.Receiver) *Component {
	t.Helper()
	registry := prometheus.NewRegistry()
	return &Component{
		opts:    component.Options{ID: "http_receive.test", Logger: util.TestLogger(t), Registerer: registry},
		metrics: newMetrics(registry),
		fanout:  alertpipeline.NewFanout([]alertpipeline.Receiver{receiver}),
		state:   handlerState{path: path, decoder: decoder, maxRequestBodySize: int64(units.MiB), forwardTimeout: time.Second},
	}
}

func serve(c *Component, method, path, body string) *httptest.ResponseRecorder {
	request := httptest.NewRequest(method, path, bytes.NewBufferString(body))
	response := httptest.NewRecorder()
	c.handleRequest(response, request)
	return response
}

func defaultArgs(path string, decoder alertpipeline.Decoder, receiver alertpipeline.Receiver) Arguments {
	var args Arguments
	args.SetToDefault()
	args.Server.HTTP.ListenPort = 0
	args.Path = path
	args.Decoder = decoder
	args.ForwardTo = []alertpipeline.Receiver{receiver}
	return args
}

func validAlert() alertpipeline.Alert {
	return alertpipeline.Alert{
		Alert: model.Alert{Labels: model.LabelSet{"alertname": "HighCPU"}, Annotations: model.LabelSet{}, StartsAt: time.Date(2026, 9, 7, 14, 0, 0, 0, time.UTC)},
		State: model.AlertFiring,
	}
}
