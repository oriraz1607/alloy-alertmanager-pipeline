package http_receive

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
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
	alertdecode "github.com/grafana/alloy/internal/component/prometheus/alertmanager/decode"
	alerthttp "github.com/grafana/alloy/internal/component/prometheus/alertmanager/http"
	alerttransform "github.com/grafana/alloy/internal/component/prometheus/alertmanager/transform"
	alertwrite "github.com/grafana/alloy/internal/component/prometheus/alertmanager/write"
	"github.com/grafana/alloy/internal/util"
)

func TestConfigurableSchemaHTTPPathDecodeAndAlertmanagerWritePipeline(t *testing.T) {
	tests := []struct {
		name       string
		path       string
		template   string
		decodeArgs alertdecode.Arguments
		assertWire func(*testing.T, map[string]any)
	}{
		{
			name: "flat boundary on alerts",
			path: "/alerts",
			template: `{
              "name": {{ to_json .Labels.alertname }},
              "metadata": {{ to_json .Labels }},
              "annotations": {{ to_json .Annotations }},
              "started": {{ to_json .StartsAt }},
              "ended": {{ to_json .EndsAt }},
              "generator": {{ to_json .GeneratorURL }},
              "status": {{ to_json .Status }}
            }`,
			decodeArgs: alertdecode.Arguments{
				LabelsFrom: ".metadata", AnnotationsFrom: ".annotations",
				Labels: map[string]string{"alertname": ".name"}, StartsAt: ".started", EndsAt: ".ended",
				GeneratorURL: ".generator", Status: ".status",
			},
			assertWire: func(t *testing.T, body map[string]any) {
				require.Equal(t, "HighCPU", body["name"])
				require.Equal(t, "production", body["metadata"].(map[string]any)["cluster"])
			},
		},
		{
			name: "nested boundary on webhook",
			path: "/webhook",
			template: `{
              "event": {"type": {{ to_json .Labels.alertname }}, "state": {{ to_json .Status }}, "timestamps": {"start": {{ to_json .StartsAt }}, "end": {{ to_json .EndsAt }}}},
              "dimensions": {{ to_json .Labels }},
              "details": {{ to_json .Annotations }},
              "source": {"url": {{ to_json .GeneratorURL }}}
            }`,
			decodeArgs: alertdecode.Arguments{
				LabelsFrom: ".dimensions", AnnotationsFrom: ".details",
				Labels: map[string]string{"alertname": ".event.type"}, StartsAt: ".event.timestamps.start", EndsAt: ".event.timestamps.end",
				GeneratorURL: ".source.url", Status: ".event.state",
			},
			assertWire: func(t *testing.T, body map[string]any) {
				require.Equal(t, "HighCPU", body["event"].(map[string]any)["type"])
				require.Equal(t, "CPU is above 90%", body["details"].(map[string]any)["summary"])
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var (
				destinationMut sync.Mutex
				destinationReq []map[string]any
			)
			destination := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				require.Equal(t, "/api/v2/alerts", r.URL.Path)
				require.Equal(t, "application/json", r.Header.Get("Content-Type"))
				var alerts []map[string]any
				require.NoError(t, json.NewDecoder(r.Body).Decode(&alerts))
				destinationMut.Lock()
				destinationReq = append(destinationReq, alerts...)
				destinationMut.Unlock()
				w.WriteHeader(http.StatusOK)
			}))
			defer destination.Close()

			writeReceiver, stopWriter := startAlertmanagerWriter(t, destination.URL)
			decodeValue := buildDecoder(t, tc.decodeArgs)
			capturedWire := make(chan []byte, 1)
			capturing := alertpipeline.DecoderFunc(func(body []byte) (alertpipeline.Alert, error) {
				capturedWire <- append([]byte(nil), body...)
				return decodeValue.Decode(body)
			})
			receiver, stopReceiver := startHTTPReceiver(t, tc.path, capturing, writeReceiver)
			transformer := buildTransformer(t, tc.template)
			senderReceiver, stopSender := startHTTPSender(t, "http://"+receiver.server.HTTPListenAddr()+tc.path, transformer)
			t.Cleanup(stopWriter)
			t.Cleanup(stopReceiver)
			t.Cleanup(stopSender)

			source := alertpipeline.Alert{
				Alert: model.Alert{
					Labels:      model.LabelSet{"alertname": "HighCPU", "severity": "critical", "instance": "server01", "cluster": "production", "namespace": "monitoring"},
					Annotations: model.LabelSet{"summary": "CPU is above 90%", "runbook": "https://example.test/runbook"},
					StartsAt:    time.Date(2026, 9, 7, 14, 0, 0, 0, time.UTC), GeneratorURL: "https://prometheus.example.test/graph",
				},
				State: model.AlertFiring,
			}
			require.NoError(t, senderReceiver.Send(t.Context(), []alertpipeline.Alert{source}))

			select {
			case wire := <-capturedWire:
				var compacted bytes.Buffer
				require.NoError(t, json.Compact(&compacted, wire))
				require.Equal(t, compacted.Bytes(), wire, "wire payload must be compact JSON")
				var object map[string]any
				require.NoError(t, json.Unmarshal(wire, &object))
				tc.assertWire(t, object)
			case <-time.After(time.Second):
				t.Fatal("custom HTTP receiver did not receive a wire packet")
			}
			require.Eventually(t, func() bool {
				destinationMut.Lock()
				defer destinationMut.Unlock()
				return len(destinationReq) == 1
			}, time.Second, 5*time.Millisecond)

			destinationMut.Lock()
			defer destinationMut.Unlock()
			alert := destinationReq[0]
			require.Equal(t, "HighCPU", alert["labels"].(map[string]any)["alertname"])
			require.Equal(t, "critical", alert["labels"].(map[string]any)["severity"])
			require.Equal(t, "monitoring", alert["labels"].(map[string]any)["namespace"])
			require.Equal(t, "CPU is above 90%", alert["annotations"].(map[string]any)["summary"])
			require.Equal(t, "https://example.test/runbook", alert["annotations"].(map[string]any)["runbook"])
			require.Equal(t, "2026-09-07T14:00:00.000Z", alert["startsAt"])
			require.Equal(t, "https://prometheus.example.test/graph", alert["generatorURL"])
		})
	}
}

func buildTransformer(t *testing.T, template string) alertpipeline.Transformer {
	t.Helper()
	var value alertpipeline.Transformer
	_, err := alerttransform.New(component.Options{
		ID: "prometheus.alertmanager.transform.integration", OnStateChange: func(exports component.Exports) { value = exports.(alerttransform.Exports).Transformer },
	}, alerttransform.Arguments{Template: template, Compact: true})
	require.NoError(t, err)
	return value
}

func buildDecoder(t *testing.T, args alertdecode.Arguments) alertpipeline.Decoder {
	t.Helper()
	var value alertpipeline.Decoder
	_, err := alertdecode.New(component.Options{
		ID: "prometheus.alertmanager.decode.integration", OnStateChange: func(exports component.Exports) { value = exports.(alertdecode.Exports).Decoder },
	}, args)
	require.NoError(t, err)
	return value
}

func startAlertmanagerWriter(t *testing.T, endpoint string) (alertpipeline.Receiver, func()) {
	t.Helper()
	var receiver alertpipeline.Receiver
	var args alertwrite.Arguments
	args.SetToDefault()
	args.Endpoint.URL = endpoint
	args.Endpoint.MinBackoff = time.Millisecond
	args.Endpoint.MaxBackoff = 2 * time.Millisecond
	args.RefreshInterval = time.Hour
	args.FiringAlertDuration = 2 * time.Hour
	args.FiringAlertTimeout = 3 * time.Hour
	args.ResolvedRetention = 0
	c, err := alertwrite.New(testOptions(t, "prometheus.alertmanager.write.integration", func(exports component.Exports) {
		receiver = exports.(alertwrite.Exports).Receiver
	}), args)
	require.NoError(t, err)
	stop := runComponent(c)
	return receiver, func() { require.NoError(t, stop()) }
}

func startHTTPReceiver(t *testing.T, path string, decoder alertpipeline.Decoder, forwardTo alertpipeline.Receiver) (*Component, func()) {
	t.Helper()
	var args Arguments
	args.SetToDefault()
	args.Server.HTTP.ListenPort = 0
	args.Path = path
	args.Decoder = decoder
	args.ForwardTo = []alertpipeline.Receiver{forwardTo}
	c, err := New(testOptions(t, "prometheus.alertmanager.http_receive.integration", func(component.Exports) {}), args)
	require.NoError(t, err)
	stop := runComponent(c)
	return c, func() { require.NoError(t, stop()) }
}

func startHTTPSender(t *testing.T, endpoint string, transformer alertpipeline.Transformer) (alertpipeline.Receiver, func()) {
	t.Helper()
	var receiver alertpipeline.Receiver
	var args alerthttp.Arguments
	args.SetToDefault()
	args.Transformer = transformer
	args.Endpoint.URL = endpoint
	args.Endpoint.MinBackoff = time.Millisecond
	args.Endpoint.MaxBackoff = 2 * time.Millisecond
	args.Queue.DrainTimeout = time.Second
	c, err := alerthttp.New(testOptions(t, "prometheus.alertmanager.http.integration", func(exports component.Exports) {
		receiver = exports.(alerthttp.Exports).Receiver
	}), args)
	require.NoError(t, err)
	stop := runComponent(c)
	return receiver, func() { require.NoError(t, stop()) }
}

func testOptions(t *testing.T, id string, onStateChange func(component.Exports)) component.Options {
	t.Helper()
	return component.Options{ID: id, Logger: util.TestLogger(t), Registerer: prometheus.NewRegistry(), Tracer: noop.NewTracerProvider(), OnStateChange: onStateChange}
}

func runComponent(c component.Component) func() error {
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- c.Run(ctx) }()
	return func() error { cancel(); return <-done }
}
