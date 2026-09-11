package http

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	stdhttp "net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/trace/noop"

	"github.com/grafana/alloy/internal/component"
	alertpipeline "github.com/grafana/alloy/internal/component/common/alertmanager"
	"github.com/grafana/alloy/internal/util"
)

func TestRegistrationAndConfiguration(t *testing.T) {
	registration, found := component.Get("prometheus.alertmanager.http")
	require.True(t, found)
	require.True(t, registration.Community)

	var args Arguments
	args.SetToDefault()
	args.Transformer = jsonTransformer("schema")
	args.Endpoint.URL = "https://site-b.example.test/api/custom-alerts"
	args.Endpoint.Timeout = 15 * time.Second
	args.Endpoint.MinBackoff = 100 * time.Millisecond
	args.Endpoint.MaxBackoff = 10 * time.Second
	args.Endpoint.MaxRetries = 5
	args.Queue.Capacity = 100
	args.Queue.DrainTimeout = 10 * time.Second
	args.Queue.BlockOnOverflow = false
	require.NoError(t, args.Validate())
	require.Equal(t, "https://site-b.example.test/api/custom-alerts", args.Endpoint.URL)
	require.Equal(t, 100, args.Queue.Capacity)
}

func TestOneAlertPerRequestAndConfiguredPath(t *testing.T) {
	for _, path := range []string{"/webhook", "/alerts", "/api/custom-alerts"} {
		t.Run(path, func(t *testing.T) {
			var mut sync.Mutex
			var requests []capturedRequest
			destination := httptest.NewServer(stdhttp.HandlerFunc(func(w stdhttp.ResponseWriter, r *stdhttp.Request) {
				body, err := io.ReadAll(r.Body)
				require.NoError(t, err)
				mut.Lock()
				requests = append(requests, capturedRequest{path: r.URL.Path, contentType: r.Header.Get("Content-Type"), body: body})
				mut.Unlock()
				w.WriteHeader(stdhttp.StatusOK)
			}))
			defer destination.Close()

			sender := startSender(t, testArguments(destination.URL+path, jsonTransformer("first")))
			require.NoError(t, sender.receiver.Send(t.Context(), []alertpipeline.Alert{testAlert("One"), testAlert("Two"), testAlert("Three")}))
			require.Eventually(t, func() bool { mut.Lock(); defer mut.Unlock(); return len(requests) == 3 }, time.Second, 5*time.Millisecond)
			mut.Lock()
			defer mut.Unlock()
			for index, request := range requests {
				require.Equal(t, path, request.path)
				require.Equal(t, "application/json", request.contentType)
				var object map[string]any
				require.NoError(t, json.Unmarshal(request.body, &object))
				require.Equal(t, []string{"One", "Two", "Three"}[index], object["name"])
				require.Equal(t, "first", object["schema"])
			}
		})
	}
}

func TestTransformationFailureRejectsWholeAdmission(t *testing.T) {
	transformer := alertpipeline.TransformerFunc(func(alert alertpipeline.Alert) ([]byte, error) {
		if alert.Labels["alertname"] == "Bad" {
			return nil, errors.New("required field missing")
		}
		return []byte(`{"ok":true}`), nil
	})
	sender := newSenderWithoutRun(t, testArguments("http://127.0.0.1:1/alerts", transformer))
	err := sender.receiver.Send(t.Context(), []alertpipeline.Alert{testAlert("Good"), testAlert("Bad")})
	require.ErrorContains(t, err, "required field missing")
	require.Zero(t, sender.component.queue.len())
}

func TestInvalidTransformerJSONIsRejected(t *testing.T) {
	transformer := alertpipeline.TransformerFunc(func(alertpipeline.Alert) ([]byte, error) { return []byte(`{"bad":`), nil })
	sender := newSenderWithoutRun(t, testArguments("http://127.0.0.1:1/alerts", transformer))
	err := sender.receiver.Send(t.Context(), []alertpipeline.Alert{testAlert("Bad")})
	require.ErrorContains(t, err, "invalid JSON")
	require.Zero(t, sender.component.queue.len())
}

func TestRetryAndPermanentStatusBehavior(t *testing.T) {
	t.Run("5xx retries", func(t *testing.T) {
		var mut sync.Mutex
		requests := 0
		destination := httptest.NewServer(stdhttp.HandlerFunc(func(w stdhttp.ResponseWriter, _ *stdhttp.Request) {
			mut.Lock()
			defer mut.Unlock()
			requests++
			if requests < 3 {
				w.WriteHeader(stdhttp.StatusServiceUnavailable)
				return
			}
			w.WriteHeader(stdhttp.StatusOK)
		}))
		defer destination.Close()
		sender := startSender(t, testArguments(destination.URL, jsonTransformer("retry")))
		require.NoError(t, sender.receiver.Send(t.Context(), []alertpipeline.Alert{testAlert("Retry")}))
		require.Eventually(t, func() bool { mut.Lock(); defer mut.Unlock(); return requests == 3 }, time.Second, 5*time.Millisecond)
	})

	t.Run("429 retries when enabled", func(t *testing.T) {
		var mut sync.Mutex
		requests := 0
		destination := httptest.NewServer(stdhttp.HandlerFunc(func(w stdhttp.ResponseWriter, _ *stdhttp.Request) {
			mut.Lock()
			defer mut.Unlock()
			requests++
			if requests == 1 {
				w.WriteHeader(stdhttp.StatusTooManyRequests)
				return
			}
			w.WriteHeader(stdhttp.StatusOK)
		}))
		defer destination.Close()
		sender := startSender(t, testArguments(destination.URL, jsonTransformer("retry-429")))
		require.NoError(t, sender.receiver.Send(t.Context(), []alertpipeline.Alert{testAlert("Retry429")}))
		require.Eventually(t, func() bool { mut.Lock(); defer mut.Unlock(); return requests == 2 }, time.Second, 5*time.Millisecond)
	})

	t.Run("4xx is permanent", func(t *testing.T) {
		requests := 0
		destination := httptest.NewServer(stdhttp.HandlerFunc(func(w stdhttp.ResponseWriter, _ *stdhttp.Request) {
			requests++
			w.WriteHeader(stdhttp.StatusBadRequest)
		}))
		defer destination.Close()
		sender := startSender(t, testArguments(destination.URL, jsonTransformer("permanent")))
		require.NoError(t, sender.receiver.Send(t.Context(), []alertpipeline.Alert{testAlert("Bad")}))
		require.Eventually(t, func() bool { return sender.component.CurrentHealth().Health == component.HealthTypeUnhealthy }, time.Second, 5*time.Millisecond)
		require.Equal(t, 1, requests)
	})
}

func TestRetryBackoffDoesNotBlockReadyAlerts(t *testing.T) {
	var mut sync.Mutex
	attempts := map[string]int{}
	var requests []string
	destination := httptest.NewServer(stdhttp.HandlerFunc(func(w stdhttp.ResponseWriter, r *stdhttp.Request) {
		var body map[string]any
		require.NoError(t, json.NewDecoder(r.Body).Decode(&body))
		name := body["name"].(string)

		mut.Lock()
		attempts[name]++
		requests = append(requests, name)
		attempt := attempts[name]
		mut.Unlock()

		if name == "One" && attempt < 3 {
			w.WriteHeader(stdhttp.StatusInternalServerError)
			return
		}
		w.WriteHeader(stdhttp.StatusOK)
	}))
	defer destination.Close()

	args := testArguments(destination.URL, jsonTransformer("scheduled-retry"))
	args.Endpoint.MinBackoff = 40 * time.Millisecond
	args.Endpoint.MaxBackoff = 80 * time.Millisecond
	args.Endpoint.MaxRetries = 2
	sender := startSender(t, args)
	require.NoError(t, sender.receiver.Send(t.Context(), []alertpipeline.Alert{
		testAlert("One"),
		testAlert("Two"),
		testAlert("Three"),
		testAlert("Four"),
	}))

	require.Eventually(t, func() bool {
		mut.Lock()
		defer mut.Unlock()
		return len(requests) == 6
	}, time.Second, 5*time.Millisecond)
	mut.Lock()
	defer mut.Unlock()
	require.Equal(t, []string{"One", "Two", "Three", "Four", "One", "One"}, requests)
}

func TestRetryExhaustionDoesNotBlockFollowingAlert(t *testing.T) {
	var mut sync.Mutex
	var requests []string
	destination := httptest.NewServer(stdhttp.HandlerFunc(func(w stdhttp.ResponseWriter, r *stdhttp.Request) {
		var body map[string]any
		require.NoError(t, json.NewDecoder(r.Body).Decode(&body))
		name := body["name"].(string)
		mut.Lock()
		requests = append(requests, name)
		mut.Unlock()
		if name == "One" {
			w.WriteHeader(stdhttp.StatusInternalServerError)
			return
		}
		w.WriteHeader(stdhttp.StatusOK)
	}))
	defer destination.Close()

	args := testArguments(destination.URL, jsonTransformer("retry-exhaustion"))
	args.Endpoint.MinBackoff = 20 * time.Millisecond
	args.Endpoint.MaxBackoff = 40 * time.Millisecond
	args.Endpoint.MaxRetries = 2
	sender := startSender(t, args)
	require.NoError(t, sender.receiver.Send(t.Context(), []alertpipeline.Alert{testAlert("One"), testAlert("Two")}))

	require.Eventually(t, func() bool {
		mut.Lock()
		defer mut.Unlock()
		return len(requests) == 4 && sender.component.CurrentHealth().Health == component.HealthTypeUnhealthy
	}, time.Second, 5*time.Millisecond)
	mut.Lock()
	defer mut.Unlock()
	require.Equal(t, []string{"One", "Two", "One", "One"}, requests)
}

func TestQueueCapacityIncludesInFlightAndDelayedPayloads(t *testing.T) {
	queue := newPayloadQueue(1)
	require.NoError(t, queue.enqueue(t.Context(), [][]byte{[]byte(`{"one":true}`)}, false))

	item, _, ready := queue.takeReady(time.Now())
	require.True(t, ready)
	require.Equal(t, 1, queue.len())
	require.ErrorIs(t, queue.enqueue(t.Context(), [][]byte{[]byte(`{"two":true}`)}, false), errQueueFull)

	queue.retry(item, time.Now().Add(time.Hour))
	require.Equal(t, 1, queue.len())
	require.ErrorIs(t, queue.enqueue(t.Context(), [][]byte{[]byte(`{"two":true}`)}, false), errQueueFull)

	_, _, ready = queue.takeReady(time.Now().Add(2 * time.Hour))
	require.True(t, ready)
	queue.complete()
	require.Zero(t, queue.len())
	require.NoError(t, queue.enqueue(t.Context(), [][]byte{[]byte(`{"two":true}`)}, false))
}

func TestConfigurationReloadChangesSubsequentBodies(t *testing.T) {
	bodies := make(chan map[string]any, 2)
	destination := httptest.NewServer(stdhttp.HandlerFunc(func(w stdhttp.ResponseWriter, r *stdhttp.Request) {
		var body map[string]any
		require.NoError(t, json.NewDecoder(r.Body).Decode(&body))
		bodies <- body
		w.WriteHeader(stdhttp.StatusOK)
	}))
	defer destination.Close()
	args := testArguments(destination.URL, jsonTransformer("one"))
	sender := startSender(t, args)
	require.NoError(t, sender.receiver.Send(t.Context(), []alertpipeline.Alert{testAlert("First")}))
	require.Equal(t, "one", (<-bodies)["schema"])

	args.Transformer = jsonTransformer("two")
	require.NoError(t, sender.component.Update(args))
	require.NoError(t, sender.receiver.Send(t.Context(), []alertpipeline.Alert{testAlert("Second")}))
	require.Equal(t, "two", (<-bodies)["schema"])
}

func TestShutdownDrainsQueue(t *testing.T) {
	received := make(chan struct{}, 1)
	destination := httptest.NewServer(stdhttp.HandlerFunc(func(w stdhttp.ResponseWriter, _ *stdhttp.Request) {
		received <- struct{}{}
		w.WriteHeader(stdhttp.StatusOK)
	}))
	defer destination.Close()
	sender := newSenderWithoutRun(t, testArguments(destination.URL, jsonTransformer("drain")))
	require.NoError(t, sender.receiver.Send(t.Context(), []alertpipeline.Alert{testAlert("Drain")}))
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- sender.component.Run(ctx) }()
	cancel()
	select {
	case <-received:
	case <-time.After(time.Second):
		t.Fatal("queued body was not drained")
	}
	require.NoError(t, <-done)
}

type capturedRequest struct {
	path        string
	contentType string
	body        []byte
}

type runningSender struct {
	component *Component
	receiver  alertpipeline.Receiver
	cancel    context.CancelFunc
	done      chan error
}

func startSender(t *testing.T, args Arguments) *runningSender {
	t.Helper()
	sender := newSenderWithoutRun(t, args)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- sender.component.Run(ctx) }()
	sender.cancel = cancel
	sender.done = done
	t.Cleanup(func() {
		cancel()
		if sender.done != nil {
			require.NoError(t, <-sender.done)
		}
	})
	require.Eventually(t, func() bool { return sender.component.CurrentHealth().Health == component.HealthTypeHealthy }, time.Second, 5*time.Millisecond)
	return sender
}

func newSenderWithoutRun(t *testing.T, args Arguments) *runningSender {
	t.Helper()
	var receiver alertpipeline.Receiver
	c, err := New(component.Options{
		ID: "prometheus.alertmanager.http.test", Logger: util.TestLogger(t), Registerer: prometheus.NewRegistry(), Tracer: noop.NewTracerProvider(),
		OnStateChange: func(exports component.Exports) { receiver = exports.(Exports).Receiver },
	}, args)
	require.NoError(t, err)
	return &runningSender{component: c, receiver: receiver}
}

func testArguments(endpoint string, transformer alertpipeline.Transformer) Arguments {
	var args Arguments
	args.SetToDefault()
	args.Transformer = transformer
	args.Endpoint.URL = endpoint
	args.Endpoint.MinBackoff = time.Millisecond
	args.Endpoint.MaxBackoff = 2 * time.Millisecond
	args.Endpoint.MaxRetries = 2
	args.Queue.DrainTimeout = time.Second
	return args
}

func jsonTransformer(schema string) alertpipeline.Transformer {
	return alertpipeline.TransformerFunc(func(alert alertpipeline.Alert) ([]byte, error) {
		return json.Marshal(map[string]string{"name": string(alert.Labels["alertname"]), "schema": schema})
	})
}

func testAlert(name string) alertpipeline.Alert {
	return alertpipeline.Alert{
		Alert: model.Alert{Labels: model.LabelSet{"alertname": model.LabelValue(name)}, Annotations: model.LabelSet{}, StartsAt: time.Date(2026, 9, 7, 14, 0, 0, 0, time.UTC)},
		State: model.AlertFiring,
	}
}
