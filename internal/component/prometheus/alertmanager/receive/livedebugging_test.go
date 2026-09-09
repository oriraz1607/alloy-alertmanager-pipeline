package receive

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/trace/noop"

	"github.com/grafana/alloy/internal/component"
	alertpipeline "github.com/grafana/alloy/internal/component/common/alertmanager"
	alertdecode "github.com/grafana/alloy/internal/component/prometheus/alertmanager/decode"
	alerthttp "github.com/grafana/alloy/internal/component/prometheus/alertmanager/http"
	httpreceive "github.com/grafana/alloy/internal/component/prometheus/alertmanager/http_receive"
	alerttransform "github.com/grafana/alloy/internal/component/prometheus/alertmanager/transform"
	alertwrite "github.com/grafana/alloy/internal/component/prometheus/alertmanager/write"
	"github.com/grafana/alloy/internal/service/livedebugging"
	"github.com/grafana/alloy/internal/util"
	"github.com/grafana/alloy/internal/util/testlivedebugging"
)

func TestLiveDebuggingPipeline(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		t.Run(map[bool]string{false: "unused", true: "subscribed"}[enabled], func(t *testing.T) {
			service := livedebugging.NewLiveDebugging()
			service.SetEnabled(true)
			host := &testlivedebugging.FakeServiceHost{ComponentsInfo: map[component.ID]testlivedebugging.FakeInfo{}}
			logs := map[string]*testlivedebugging.Log{}
			options := func(name string, export func(component.Exports)) component.Options {
				return component.Options{ID: "prometheus.alertmanager." + name + ".test", Logger: util.TestLogger(t), Registerer: prometheus.NewRegistry(), Tracer: noop.NewTracerProvider(), OnStateChange: export,
					GetServiceData: func(name string) (any, error) { require.Equal(t, livedebugging.ServiceName, name); return service, nil }}
			}
			register := func(name string, c component.Component) {
				id := component.ParseID("prometheus.alertmanager." + name + ".test")
				host.ComponentsInfo[id] = testlivedebugging.FakeInfo{ComponentName: "prometheus.alertmanager." + name, Component: c}
				require.Implements(t, (*component.LiveDebugging)(nil), c)
				log := testlivedebugging.NewLog()
				logs[name] = log
				if enabled {
					require.NoError(t, service.AddCallback(host, livedebugging.CallbackID(name), livedebugging.ComponentID(id.String()), func(d livedebugging.Data) {
						require.Equal(t, livedebugging.AlertmanagerAlert, d.Type)
						log.Append(d.DataFunc())
					}))
				}
			}
			run := func(c component.Component) {
				ctx, cancel := context.WithCancel(context.Background())
				done := make(chan error, 1)
				go func() { done <- c.Run(ctx) }()
				t.Cleanup(func() { cancel(); require.NoError(t, <-done) })
			}
			delivered := make(chan []byte, 2)
			destination := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				require.Equal(t, "/api/v2/alerts", r.URL.Path)
				body, err := io.ReadAll(r.Body)
				require.NoError(t, err)
				delivered <- body
			}))
			defer destination.Close()
			var writeReceiver alertpipeline.Receiver
			var wa alertwrite.Arguments
			wa.SetToDefault()
			wa.Endpoint.URL = destination.URL
			wa.RefreshInterval = time.Hour
			wa.FiringAlertDuration = 2 * time.Hour
			wa.FiringAlertTimeout = 3 * time.Hour
			writer, err := alertwrite.New(options("write", func(e component.Exports) { writeReceiver = e.(alertwrite.Exports).Receiver }), wa)
			require.NoError(t, err)
			register("write", writer)
			run(writer)
			var decoder alertpipeline.Decoder
			dc, err := alertdecode.New(options("decode", func(e component.Exports) { decoder = e.(alertdecode.Exports).Decoder }), alertdecode.Arguments{
				StartsAt: ".startsAt", LabelsFrom: ".labels", Labels: map[string]string{"alertname": ".alertname"},
			})
			require.NoError(t, err)
			register("decode", dc)
			var ra httpreceive.Arguments
			ra.SetToDefault()
			ra.Server.HTTP.ListenPort = 0
			ra.Path = "/alerts"
			ra.Decoder = decoder
			ra.ForwardTo = []alertpipeline.Receiver{writeReceiver}
			// Capture the exported receiver's logical request through a real listener.
			// A free port is obtained before New, which owns the listener lifecycle.
			listener := httptest.NewServer(http.NotFoundHandler())
			addr := listener.Listener.Addr().(*net.TCPAddr)
			port := addr.Port
			listener.Close()
			ra.Server.HTTP.ListenAddress = "127.0.0.1"
			ra.Server.HTTP.ListenPort = port
			hr, err := httpreceive.New(options("http_receive", func(component.Exports) {}), ra)
			require.NoError(t, err)
			register("http_receive", hr)
			run(hr)
			var transformer alertpipeline.Transformer
			tr, err := alerttransform.New(options("transform", func(e component.Exports) { transformer = e.(alerttransform.Exports).Transformer }), alerttransform.Arguments{
				Template: `{ "alertname": {{to_json .Labels.alertname}}, "labels": {"severity": {{to_json .Labels.severity}}}, "startsAt": {{to_json .StartsAt}} }`, Compact: false,
			})
			require.NoError(t, err)
			register("transform", tr)
			var senderReceiver alertpipeline.Receiver
			var ha alerthttp.Arguments
			ha.SetToDefault()
			ha.Transformer = transformer
			ha.Endpoint.URL = fmt.Sprintf("http://127.0.0.1:%d/alerts", port)
			sender, err := alerthttp.New(options("http", func(e component.Exports) { senderReceiver = e.(alerthttp.Exports).Receiver }), ha)
			require.NoError(t, err)
			register("http", sender)
			run(sender)
			var args Arguments
			args.SetToDefault()
			args.Server.HTTP.ListenPort = 0
			args.ForwardTo = []alertpipeline.Receiver{senderReceiver}
			receiver, err := New(options("receive", func(component.Exports) {}), args)
			require.NoError(t, err)
			register("receive", receiver)
			run(receiver)
			payload := `{"alerts":[{"status":"firing","labels":{"alertname":"TestAlert","severity":"critical"},"annotations":{"summary":"test"},"startsAt":"2026-09-09T01:00:00Z","generatorURL":"http://prometheus/graph"}],"version":"4"}`
			require.Equal(t, http.StatusOK, serveWebhook(receiver, http.MethodPost, payload).Code)
			select {
			case body := <-delivered:
				var alerts []map[string]any
				require.NoError(t, json.Unmarshal(body, &alerts))
				require.Len(t, alerts, 1)
				require.Equal(t, map[string]any{"alertname": "TestAlert", "severity": "critical"}, alerts[0]["labels"])
			case <-time.After(5 * time.Second):
				t.Fatal("alert not delivered")
			}
			if !enabled {
				for _, log := range logs {
					require.Empty(t, log.Get())
				}
				return
			}
			require.Eventually(t, func() bool { return strings.Contains(strings.Join(logs["http"].Get(), "\n"), "HTTP RESPONSE") }, time.Second, time.Millisecond)
			expected := map[string][]string{
				"receive":      {"[IN] WEBHOOK", "[OUT]", `"status":"firing"`, `"summary":"test"`},
				"transform":    {"[IN]", "[OUT]", `"alertname":"TestAlert"`},
				"http":         {"HTTP REQUEST", "POST " + ha.Endpoint.URL, "Path: /alerts", "HTTP RESPONSE", "Status: 200 OK"},
				"http_receive": {"HTTP RECEIVED", "Path: /alerts"},
				"decode":       {"[IN]", "[OUT]", `"labels":{"alertname":"TestAlert","severity":"critical"}`},
				"write":        {"[IN]", "[OUT] ALERTMANAGER REQUEST", `"labels":{"alertname":"TestAlert","severity":"critical"}`},
			}
			for name, parts := range expected {
				text := strings.Join(logs[name].Get(), "\n")
				for _, part := range parts {
					require.Contains(t, text, part, name)
				}
			}
			actualBody := `{ "alertname": "TestAlert", "labels": {"severity": "critical"}, "startsAt": "2026-09-09T01:00:00Z" }`
			for _, name := range []string{"transform", "http", "http_receive", "decode"} {
				require.Contains(t, strings.Join(logs[name].Get(), "\n"), actualBody, name)
			}
		})
	}
}
