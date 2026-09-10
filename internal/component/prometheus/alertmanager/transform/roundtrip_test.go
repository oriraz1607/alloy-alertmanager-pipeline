package transform

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/grafana/alloy/internal/component"
	alertpipeline "github.com/grafana/alloy/internal/component/common/alertmanager"
	"github.com/grafana/alloy/internal/component/prometheus/alertmanager/decode"
	"github.com/grafana/alloy/internal/service/livedebugging"
	"github.com/grafana/alloy/internal/util/testlivedebugging"
)

func TestStringLabelsHTTPRoundTrip(t *testing.T) {
	for _, compact := range []bool{false, true} {
		original := testAlert()
		original.Labels["custom_boundary_label"] = "disk: almost, full { } \" \\ % שלום\n\t"
		original.Labels["empty"] = ""
		transformer := configuredTransformer(t, `{
 "labels": "{{ .Labels | to_string }}",
 "annotations": {{ .Annotations | to_json }},
 "started": {{ .StartsAt | to_json }},
 "ended": {{ .EndsAt | to_json }},
 "generator": {{ .GeneratorURL | to_json }},
 "status": {{ .Status | to_json }},
 "fingerprint": {{ .SourceFingerprint | to_json }}
}`)
		transformer.compact = compact
		svc := livedebugging.NewLiveDebugging()
		svc.SetEnabled(true)
		host := &testlivedebugging.FakeServiceHost{ComponentsInfo: map[component.ID]testlivedebugging.FakeInfo{}}
		log := testlivedebugging.NewLog()
		for _, id := range []string{"prometheus.alertmanager.transform.test", "prometheus.alertmanager.decode.test"} {
			host.ComponentsInfo[component.ParseID(id)] = testlivedebugging.FakeInfo{ComponentName: strings.TrimSuffix(id, ".test"), Component: &testlivedebugging.FakeComponentLiveDebugging{}}
			require.NoError(t, svc.AddCallback(host, livedebugging.CallbackID(id), livedebugging.ComponentID(id), func(d livedebugging.Data) { log.Append(d.DataFunc()) }))
		}
		service := func(string) (any, error) { return svc, nil }
		transformer.debugDataPublisher = alertpipeline.NewDebugPublisher(component.Options{ID: "prometheus.alertmanager.transform.test", GetServiceData: service})
		var decoder alertpipeline.Decoder
		_, err := decode.New(component.Options{ID: "prometheus.alertmanager.decode.test", GetServiceData: service, OnStateChange: func(exports component.Exports) { decoder = exports.(decode.Exports).Decoder }}, decode.Arguments{
			LabelsFrom: ".labels", LabelsFormat: "to_string", AnnotationsFrom: ".annotations", StartsAt: ".started", EndsAt: ".ended", GeneratorURL: ".generator", Status: ".status", SourceFingerprint: ".fingerprint",
		})
		require.NoError(t, err)
		body, err := transformer.Transform(original)
		require.NoError(t, err)
		var wire map[string]any
		require.NoError(t, json.Unmarshal(body, &wire))
		require.IsType(t, "", wire["labels"])
		received := make(chan []byte, 1)
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			data, err := io.ReadAll(r.Body)
			if err != nil {
				w.WriteHeader(500)
				return
			}
			received <- data
		}))
		t.Cleanup(server.Close)
		response, err := server.Client().Post(server.URL, "application/json", bytes.NewReader(body))
		require.NoError(t, err)
		require.Equal(t, http.StatusOK, response.StatusCode)
		response.Body.Close()
		server.Close()
		actual := <-received
		require.Equal(t, body, actual)
		restored, err := decoder.Decode(actual)
		require.NoError(t, err)
		require.Equal(t, original, restored)
		debug := strings.Join(log.Get(), "\n")
		require.Contains(t, debug, "[OUT] "+string(body))
		require.Contains(t, debug, "[IN] "+string(body))
		labelsJSON, err := json.Marshal(original.Labels)
		require.NoError(t, err)
		require.Contains(t, debug, `[OUT] {"labels":`+string(labelsJSON))
	}
}
