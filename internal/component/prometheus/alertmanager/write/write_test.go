package write

import (
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/trace/noop"
	"go.uber.org/atomic"

	"github.com/grafana/alloy/internal/component"
	alertpipeline "github.com/grafana/alloy/internal/component/common/alertmanager"
	commonconfig "github.com/grafana/alloy/internal/component/common/config"
	"github.com/grafana/alloy/internal/util"
	"github.com/grafana/alloy/syntax"
	"github.com/grafana/alloy/syntax/alloytypes"
)

func TestRegistrationAndConfiguration(t *testing.T) {
	registration, found := component.Get("prometheus.alertmanager.write")
	require.True(t, found)
	require.True(t, registration.Community)

	var args Arguments
	require.NoError(t, syntax.Unmarshal([]byte(`
      refresh_interval     = "1m"
      firing_alert_duration = "5m"
      firing_alert_timeout = "5h"
      resolved_retention   = "5m"

      endpoint {
        url                = "https://alertmanager.example.com/api/v2/alerts"
        timeout            = "15s"
        batch_size         = 1
        batch_wait         = "500ms"
        min_backoff_period = "100ms"
        max_backoff_period = "10s"
        max_retries        = 5
        retry_on_http_429  = true

        tls_config {
          insecure_skip_verify = true
        }
      }

      queue_config {
        capacity         = 100
        drain_timeout    = "10s"
        block_on_overflow = false
      }
    `), &args))
	require.Equal(t, 1, args.Endpoint.BatchSize)
	require.Equal(t, 100, args.Queue.Capacity)
	require.False(t, args.Queue.BlockOnOverflow)
	require.True(t, args.Endpoint.HTTPClientConfig.TLSConfig.InsecureSkipVerify)
}

func TestSingleAlertRequestPreservesFields(t *testing.T) {
	var (
		mut     sync.Mutex
		request capturedRequest
	)
	destination := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		mut.Lock()
		request = capturedRequest{method: r.Method, path: r.URL.Path, contentType: r.Header.Get("Content-Type"), body: body}
		mut.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	defer destination.Close()

	args := testArguments(destination.URL)
	writer := startWriter(t, args)
	require.NoError(t, writer.receiver.Send(t.Context(), []alertpipeline.Alert{testAlert("HighCPU", model.AlertFiring)}))
	require.Eventually(t, func() bool {
		mut.Lock()
		defer mut.Unlock()
		return len(request.body) > 0
	}, time.Second, 5*time.Millisecond)

	mut.Lock()
	defer mut.Unlock()
	require.Equal(t, http.MethodPost, request.method)
	require.Equal(t, defaultAlertsPath, request.path)
	require.Equal(t, "application/json", request.contentType)
	var alerts []map[string]any
	require.NoError(t, json.Unmarshal(request.body, &alerts))
	require.Len(t, alerts, 1)
	require.Equal(t, "HighCPU", alerts[0]["labels"].(map[string]any)["alertname"])
	require.Equal(t, "summary text", alerts[0]["annotations"].(map[string]any)["summary"])
	require.Equal(t, "http://prometheus.example/graph", alerts[0]["generatorURL"])
	require.NotContains(t, string(request.body), `"status"`)
	require.NotContains(t, string(request.body), `"fingerprint"`)
	endsAt, err := time.Parse(time.RFC3339Nano, alerts[0]["endsAt"].(string))
	require.NoError(t, err)
	require.Greater(t, endsAt, time.Now())
}

func TestBatchingAndSingleAlertMode(t *testing.T) {
	tests := []struct {
		name         string
		batchSize    int
		wantRequests int
		wantCounts   []int
	}{
		{name: "one alert per request", batchSize: 1, wantRequests: 2, wantCounts: []int{1, 1}},
		{name: "two alerts per request", batchSize: 2, wantRequests: 1, wantCounts: []int{2}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var mut sync.Mutex
			var counts []int
			destination := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var alerts []map[string]any
				require.NoError(t, json.NewDecoder(r.Body).Decode(&alerts))
				mut.Lock()
				counts = append(counts, len(alerts))
				mut.Unlock()
				w.WriteHeader(http.StatusOK)
			}))
			defer destination.Close()

			args := testArguments(destination.URL)
			args.Endpoint.BatchSize = tc.batchSize
			writer := startWriter(t, args)
			require.NoError(t, writer.receiver.Send(t.Context(), []alertpipeline.Alert{
				testAlert("One", model.AlertFiring),
				testAlert("Two", model.AlertFiring),
			}))
			require.Eventually(t, func() bool {
				mut.Lock()
				defer mut.Unlock()
				return len(counts) == tc.wantRequests
			}, time.Second, 5*time.Millisecond)
			mut.Lock()
			defer mut.Unlock()
			require.Equal(t, tc.wantCounts, counts)
		})
	}
}

func TestDestinationStatusAndRetries(t *testing.T) {
	t.Run("4xx is permanent", func(t *testing.T) {
		var requests atomic.Int64
		destination := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			requests.Add(1)
			w.WriteHeader(http.StatusBadRequest)
		}))
		defer destination.Close()
		writer := startWriter(t, testArguments(destination.URL))
		require.NoError(t, writer.receiver.Send(t.Context(), []alertpipeline.Alert{testAlert("Bad", model.AlertFiring)}))
		require.Eventually(t, func() bool { return writer.component.CurrentHealth().Health == component.HealthTypeUnhealthy }, time.Second, 5*time.Millisecond)
		require.EqualValues(t, 1, requests.Load())
	})

	t.Run("5xx is retried", func(t *testing.T) {
		var mut sync.Mutex
		requests := 0
		destination := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			mut.Lock()
			defer mut.Unlock()
			requests++
			if requests < 3 {
				w.WriteHeader(http.StatusServiceUnavailable)
				return
			}
			w.WriteHeader(http.StatusOK)
		}))
		defer destination.Close()
		args := testArguments(destination.URL)
		args.Endpoint.MaxRetries = 2
		writer := startWriter(t, args)
		require.NoError(t, writer.receiver.Send(t.Context(), []alertpipeline.Alert{testAlert("Retry", model.AlertFiring)}))
		require.Eventually(t, func() bool {
			mut.Lock()
			defer mut.Unlock()
			return requests == 3
		}, time.Second, 5*time.Millisecond)
		require.Eventually(t, func() bool { return writer.component.CurrentHealth().Health == component.HealthTypeHealthy }, time.Second, 5*time.Millisecond)
	})

	t.Run("connection failure is retried then reported", func(t *testing.T) {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		endpoint := "http://" + listener.Addr().String()
		require.NoError(t, listener.Close())
		args := testArguments(endpoint)
		args.Endpoint.MaxRetries = 1
		writer := startWriter(t, args)
		require.NoError(t, writer.receiver.Send(t.Context(), []alertpipeline.Alert{testAlert("Unavailable", model.AlertFiring)}))
		require.Eventually(t, func() bool { return writer.component.CurrentHealth().Health == component.HealthTypeUnhealthy }, time.Second, 5*time.Millisecond)
	})
}

func TestAuthenticationAndTLS(t *testing.T) {
	t.Run("basic auth", func(t *testing.T) {
		accepted := make(chan struct{}, 1)
		destination := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			username, password, ok := r.BasicAuth()
			require.True(t, ok)
			require.Equal(t, "alloy", username)
			require.Equal(t, "secret", password)
			accepted <- struct{}{}
			w.WriteHeader(http.StatusOK)
		}))
		defer destination.Close()
		args := testArguments(destination.URL)
		args.Endpoint.HTTPClientConfig.BasicAuth = &commonconfig.BasicAuth{Username: "alloy", Password: alloytypes.Secret("secret")}
		writer := startWriter(t, args)
		require.NoError(t, writer.receiver.Send(t.Context(), []alertpipeline.Alert{testAlert("Auth", model.AlertFiring)}))
		select {
		case <-accepted:
		case <-time.After(time.Second):
			t.Fatal("destination did not receive authenticated request")
		}
	})

	t.Run("TLS verification can be configured", func(t *testing.T) {
		destination := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) }))
		defer destination.Close()
		args := testArguments(destination.URL)
		args.Endpoint.HTTPClientConfig.TLSConfig.InsecureSkipVerify = true
		writer := startWriter(t, args)
		require.NoError(t, writer.receiver.Send(t.Context(), []alertpipeline.Alert{testAlert("TLS", model.AlertFiring)}))
		require.Eventually(t, func() bool { return writer.component.CurrentHealth().Health == component.HealthTypeHealthy }, time.Second, 5*time.Millisecond)
	})
}

func TestQueueLimits(t *testing.T) {
	args := testArguments("http://127.0.0.1:1")
	args.Queue.Capacity = 1
	args.Queue.BlockOnOverflow = false
	writer := newWriterWithoutRun(t, args)
	require.NoError(t, writer.receiver.Send(t.Context(), []alertpipeline.Alert{testAlert("One", model.AlertFiring)}))
	require.ErrorIs(t, writer.receiver.Send(t.Context(), []alertpipeline.Alert{testAlert("Two", model.AlertFiring)}), errQueueFull)
	require.ErrorIs(t, writer.receiver.Send(t.Context(), []alertpipeline.Alert{
		testAlert("Two", model.AlertFiring), testAlert("Three", model.AlertFiring),
	}), errQueueFull)
}

func TestQueueBlockingHonorsContext(t *testing.T) {
	args := testArguments("http://127.0.0.1:1")
	args.Queue.Capacity = 1
	args.Queue.BlockOnOverflow = true
	writer := newWriterWithoutRun(t, args)
	require.NoError(t, writer.receiver.Send(t.Context(), []alertpipeline.Alert{testAlert("One", model.AlertFiring)}))
	ctx, cancel := context.WithTimeout(t.Context(), time.Millisecond)
	defer cancel()
	require.ErrorIs(t, writer.receiver.Send(ctx, []alertpipeline.Alert{testAlert("Two", model.AlertFiring)}), context.DeadlineExceeded)
}

func TestFiringRefreshAndResolvedRetention(t *testing.T) {
	var mut sync.Mutex
	var bodies [][]map[string]any
	destination := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var alerts []map[string]any
		require.NoError(t, json.NewDecoder(r.Body).Decode(&alerts))
		mut.Lock()
		bodies = append(bodies, alerts)
		mut.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	defer destination.Close()

	args := testArguments(destination.URL)
	args.RefreshInterval = 15 * time.Millisecond
	args.FiringAlertDuration = 10 * time.Second
	args.FiringAlertTimeout = time.Second
	args.ResolvedRetention = 45 * time.Millisecond
	writer := startWriter(t, args)
	firing := testAlert("Refresh", model.AlertFiring)
	require.NoError(t, writer.receiver.Send(t.Context(), []alertpipeline.Alert{firing}))
	require.Eventually(t, func() bool {
		mut.Lock()
		defer mut.Unlock()
		return len(bodies) >= 2
	}, time.Second, 5*time.Millisecond)

	resolved := firing
	resolved.State = model.AlertResolved
	resolved.EndsAt = time.Now().Add(-time.Second)
	require.NoError(t, writer.receiver.Send(t.Context(), []alertpipeline.Alert{resolved}))
	require.Eventually(t, func() bool {
		firingCount, resolvedCount := writer.component.tracker.counts()
		return firingCount == 0 && resolvedCount == 0
	}, time.Second, 5*time.Millisecond)

	mut.Lock()
	defer mut.Unlock()
	var sawResolved bool
	for _, batch := range bodies {
		for _, alert := range batch {
			endsAt, err := time.Parse(time.RFC3339Nano, alert["endsAt"].(string))
			require.NoError(t, err)
			if !endsAt.After(time.Now()) {
				sawResolved = true
			}
		}
	}
	require.True(t, sawResolved)
}

func TestUpdateRefreshIntervalWakesScheduler(t *testing.T) {
	var requests atomic.Int64
	destination := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests.Inc()
		w.WriteHeader(http.StatusOK)
	}))
	defer destination.Close()

	args := testArguments(destination.URL)
	writer := startWriter(t, args)
	require.NoError(t, writer.receiver.Send(t.Context(), []alertpipeline.Alert{testAlert("Reload", model.AlertFiring)}))
	require.Eventually(t, func() bool { return requests.Load() == 1 }, time.Second, 5*time.Millisecond)

	args.RefreshInterval = 10 * time.Millisecond
	args.FiringAlertDuration = time.Second
	args.FiringAlertTimeout = time.Minute
	require.NoError(t, writer.component.Update(args))
	require.Eventually(t, func() bool { return requests.Load() >= 2 }, time.Second, 5*time.Millisecond)
}

func TestStaleFiringStateExpires(t *testing.T) {
	destination := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) }))
	defer destination.Close()
	args := testArguments(destination.URL)
	args.RefreshInterval = 10 * time.Millisecond
	args.FiringAlertDuration = 30 * time.Millisecond
	args.FiringAlertTimeout = 25 * time.Millisecond
	writer := startWriter(t, args)
	require.NoError(t, writer.receiver.Send(t.Context(), []alertpipeline.Alert{testAlert("Stale", model.AlertFiring)}))
	require.Eventually(t, func() bool {
		firing, _ := writer.component.tracker.counts()
		return firing == 0
	}, time.Second, 5*time.Millisecond)
}

func TestShutdownDrainsQueue(t *testing.T) {
	requests := make(chan struct{}, 1)
	destination := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests <- struct{}{}
		w.WriteHeader(http.StatusOK)
	}))
	defer destination.Close()
	args := testArguments(destination.URL)
	args.Endpoint.BatchWait = time.Hour
	writer := startWriter(t, args)
	require.NoError(t, writer.receiver.Send(t.Context(), []alertpipeline.Alert{testAlert("Drain", model.AlertFiring)}))
	writer.cancel()
	select {
	case <-requests:
	case <-time.After(time.Second):
		t.Fatal("queued alert was not drained")
	}
	require.NoError(t, <-writer.done)
	writer.done = nil
}

func TestMarshalResolvedWithFutureEndsAt(t *testing.T) {
	now := time.Now()
	alert := testAlert("Resolved", model.AlertResolved)
	alert.EndsAt = now.Add(time.Hour)
	body, err := marshalAlerts([]alertpipeline.Alert{alert}, now, time.Minute)
	require.NoError(t, err)
	var alerts []map[string]any
	require.NoError(t, json.Unmarshal(body, &alerts))
	endsAt, err := time.Parse(time.RFC3339Nano, alerts[0]["endsAt"].(string))
	require.NoError(t, err)
	require.WithinDuration(t, now, endsAt, time.Millisecond)
}

type capturedRequest struct {
	method      string
	path        string
	contentType string
	body        []byte
}

type runningWriter struct {
	component *Component
	receiver  alertpipeline.Receiver
	cancel    context.CancelFunc
	done      chan error
}

func startWriter(t *testing.T, args Arguments) *runningWriter {
	t.Helper()
	writer := newWriterWithoutRun(t, args)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- writer.component.Run(ctx) }()
	writer.cancel = cancel
	writer.done = done
	t.Cleanup(func() {
		cancel()
		if writer.done != nil {
			require.NoError(t, <-writer.done)
		}
	})
	require.Eventually(t, func() bool { return writer.component.CurrentHealth().Health == component.HealthTypeHealthy }, time.Second, 5*time.Millisecond)
	return writer
}

func newWriterWithoutRun(t *testing.T, args Arguments) *runningWriter {
	t.Helper()
	var receiver alertpipeline.Receiver
	c, err := New(component.Options{
		ID:         "prometheus.alertmanager.write.test",
		Logger:     util.TestLogger(t),
		Registerer: prometheus.NewRegistry(),
		Tracer:     noop.NewTracerProvider(),
		OnStateChange: func(exports component.Exports) {
			receiver = exports.(Exports).Receiver
		},
	}, args)
	require.NoError(t, err)
	require.NotNil(t, receiver)
	return &runningWriter{component: c, receiver: receiver}
}

func testArguments(endpoint string) Arguments {
	var args Arguments
	args.SetToDefault()
	args.Endpoint.URL = endpoint
	args.Endpoint.BatchWait = 5 * time.Millisecond
	args.Endpoint.MinBackoff = time.Millisecond
	args.Endpoint.MaxBackoff = 2 * time.Millisecond
	args.Endpoint.MaxRetries = 2
	args.RefreshInterval = time.Hour
	args.FiringAlertDuration = 2 * time.Hour
	args.FiringAlertTimeout = 3 * time.Hour
	args.ResolvedRetention = 0
	return args
}

func testAlert(name string, state model.AlertStatus) alertpipeline.Alert {
	return alertpipeline.Alert{
		Alert: model.Alert{
			Labels:       model.LabelSet{"alertname": model.LabelValue(name), "instance": "test"},
			Annotations:  model.LabelSet{"summary": "summary text"},
			StartsAt:     time.Date(2026, 8, 25, 8, 0, 0, 0, time.UTC),
			GeneratorURL: "http://prometheus.example/graph",
		},
		State: state,
	}
}
