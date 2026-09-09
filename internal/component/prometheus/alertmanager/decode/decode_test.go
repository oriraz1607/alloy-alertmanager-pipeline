package decode

import (
	"testing"
	"time"

	"github.com/prometheus/common/model"
	"github.com/stretchr/testify/require"

	"github.com/grafana/alloy/internal/component"
	"github.com/grafana/alloy/syntax"
)

func TestRegistrationAndConfiguration(t *testing.T) {
	registration, found := component.Get("prometheus.alertmanager.decode")
	require.True(t, found)
	require.True(t, registration.Community)

	var args Arguments
	require.NoError(t, syntax.Unmarshal([]byte(`
      labels_from      = ".metadata"
      annotations_from = ".annotations"
      labels = { alertname = ".event.name" }
      annotations = { summary = ".message.text" }
      starts_at    = ".timestamps.started"
      ends_at      = ".timestamps.ended"
      generator_url = ".generator"
      status        = ".status"
    `), &args))
	require.NoError(t, args.Validate())
	require.Equal(t, ".metadata", args.LabelsFrom)
}

func TestWholeMapImportAndExplicitOverride(t *testing.T) {
	decoder := configuredDecoder(t, Arguments{
		LabelsFrom:      ".metadata",
		AnnotationsFrom: ".annotations",
		Labels:          map[string]string{"alertname": ".event.name"},
		Annotations:     map[string]string{"summary": ".message"},
		StartsAt:        ".timestamps.started",
		EndsAt:          ".timestamps.ended",
		GeneratorURL:    ".generator",
		Status:          ".status",
	})
	body := []byte(`{
      "event":{"name":"HighCPU"},
      "metadata":{"alertname":"WrongName","severity":"critical","instance":"server01","cluster":"production","namespace":"monitoring"},
      "annotations":{"runbook":"https://example.test/runbook","new_annotation":"automatic"},
      "message":"CPU is high",
      "timestamps":{"started":"2026-09-07T14:00:00Z","ended":"2026-09-07T15:00:00Z"},
      "generator":"https://example.test/graph",
      "status":"firing"
    }`)
	alert, err := decoder.Decode(body)
	require.NoError(t, err)
	require.Equal(t, model.LabelValue("HighCPU"), alert.Labels["alertname"])
	require.Equal(t, model.LabelValue("critical"), alert.Labels["severity"])
	require.Equal(t, model.LabelValue("monitoring"), alert.Labels["namespace"])
	require.Equal(t, model.LabelValue("CPU is high"), alert.Annotations["summary"])
	require.Equal(t, model.LabelValue("automatic"), alert.Annotations["new_annotation"])
	require.Equal(t, time.Date(2026, 9, 7, 14, 0, 0, 0, time.UTC), alert.StartsAt)
	require.Equal(t, time.Date(2026, 9, 7, 15, 0, 0, 0, time.UTC), alert.EndsAt)
	require.Equal(t, "https://example.test/graph", alert.GeneratorURL)
	require.Equal(t, model.AlertFiring, alert.State)
}

func TestNewMapFieldsImportWithoutMappingChanges(t *testing.T) {
	decoder := configuredDecoder(t, Arguments{LabelsFrom: ".labels", StartsAt: ".startsAt"})
	first, err := decoder.Decode([]byte(`{"labels":{"alertname":"HighCPU","severity":"critical","instance":"server01"},"startsAt":"2026-09-07T14:00:00Z"}`))
	require.NoError(t, err)
	require.Len(t, first.Labels, 3)

	second, err := decoder.Decode([]byte(`{"labels":{"alertname":"HighCPU","severity":"critical","instance":"server01","cluster":"production","namespace":"monitoring"},"startsAt":"2026-09-07T14:00:00Z"}`))
	require.NoError(t, err)
	require.Len(t, second.Labels, 5)
	require.Equal(t, model.LabelValue("production"), second.Labels["cluster"])
	require.Equal(t, model.LabelValue("monitoring"), second.Labels["namespace"])
}

func TestDifferentSchemasProduceSameAlert(t *testing.T) {
	first := configuredDecoder(t, Arguments{
		LabelsFrom:  ".labels",
		Labels:      map[string]string{"alertname": ".alertname"},
		Annotations: map[string]string{"summary": ".summary"},
		StartsAt:    ".startsAt",
	})
	second := configuredDecoder(t, Arguments{
		LabelsFrom:  ".metadata",
		Labels:      map[string]string{"alertname": ".event.type"},
		Annotations: map[string]string{"summary": ".message.text"},
		StartsAt:    ".event.started",
	})
	a, err := first.Decode([]byte(`{"alertname":"DiskFull","summary":"Disk full","labels":{"severity":"warning","instance":"server02"},"startsAt":"2026-09-07T15:00:00Z"}`))
	require.NoError(t, err)
	b, err := second.Decode([]byte(`{"event":{"type":"DiskFull","started":"2026-09-07T15:00:00Z"},"metadata":{"severity":"warning","instance":"server02"},"message":{"text":"Disk full"}}`))
	require.NoError(t, err)
	require.Equal(t, a, b)
}

func TestOptionalWholeMapPathAndStatusInference(t *testing.T) {
	decoder := configuredDecoder(t, Arguments{
		LabelsFrom:      ".optional",
		Labels:          map[string]string{"alertname": ".name"},
		AnnotationsFrom: ".missing_annotations",
		StartsAt:        ".started",
		EndsAt:          ".ended",
	})
	alert, err := decoder.Decode([]byte(`{"name":"Done","started":"2019-01-01T00:00:00Z","ended":"2020-01-01T00:00:00Z"}`))
	require.NoError(t, err)
	require.Equal(t, model.AlertResolved, alert.State)
	require.Empty(t, alert.Annotations)
}

func TestDecodeFailures(t *testing.T) {
	tests := []struct {
		name string
		args Arguments
		body string
		want string
	}{
		{name: "invalid JSON", args: Arguments{LabelsFrom: ".labels", StartsAt: ".startsAt"}, body: `{"labels":`, want: "decoding JSON object"},
		{name: "non-object root", args: Arguments{LabelsFrom: ".labels", StartsAt: ".startsAt"}, body: `[]`, want: "JSON object"},
		{name: "trailing value", args: Arguments{LabelsFrom: ".labels", StartsAt: ".startsAt"}, body: `{"labels":{"a":"b"}} {}`, want: "exactly one"},
		{name: "wrong map type", args: Arguments{LabelsFrom: ".metadata", StartsAt: ".startsAt"}, body: `{"metadata":"critical"}`, want: "must select a JSON object"},
		{name: "wrong annotations map type", args: Arguments{LabelsFrom: ".labels", AnnotationsFrom: ".annotations", StartsAt: ".startsAt"}, body: `{"labels":{"alertname":"A"},"annotations":[],"startsAt":"2026-09-07T14:00:00Z"}`, want: "annotations_from path .annotations must select a JSON object"},
		{name: "non-string label", args: Arguments{LabelsFrom: ".metadata", StartsAt: ".startsAt"}, body: `{"metadata":{"severity":5}}`, want: "must contain a JSON string"},
		{name: "non-string annotation", args: Arguments{LabelsFrom: ".labels", AnnotationsFrom: ".annotations", StartsAt: ".startsAt"}, body: `{"labels":{"alertname":"A"},"annotations":{"count":5},"startsAt":"2026-09-07T14:00:00Z"}`, want: "must contain a JSON string"},
		{name: "null label", args: Arguments{LabelsFrom: ".metadata", StartsAt: ".startsAt"}, body: `{"metadata":{"owner":null}}`, want: "must contain a JSON string"},
		{name: "missing explicit", args: Arguments{Labels: map[string]string{"alertname": ".name"}, StartsAt: ".startsAt"}, body: `{}`, want: "is missing"},
		{name: "wrong explicit type", args: Arguments{Labels: map[string]string{"alertname": ".name"}, StartsAt: ".startsAt"}, body: `{"name":true}`, want: "must select a JSON string"},
		{name: "bad timestamp", args: Arguments{LabelsFrom: ".labels", StartsAt: ".started"}, body: `{"labels":{"alertname":"A"},"started":"yesterday"}`, want: "RFC3339"},
		{name: "bad end timestamp", args: Arguments{LabelsFrom: ".labels", StartsAt: ".started", EndsAt: ".ended"}, body: `{"labels":{"alertname":"A"},"started":"2026-09-07T14:00:00Z","ended":"later"}`, want: "RFC3339"},
		{name: "invalid status", args: Arguments{LabelsFrom: ".labels", StartsAt: ".startsAt", Status: ".status"}, body: `{"labels":{"alertname":"A"},"startsAt":"2026-09-07T14:00:00Z","status":"pending"}`, want: "alert state"},
		{name: "empty alert", args: Arguments{LabelsFrom: ".labels", StartsAt: ".startsAt"}, body: `{"startsAt":"2026-09-07T14:00:00Z"}`, want: "decoded alert is invalid"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			decoder := configuredDecoder(t, tc.args)
			_, err := decoder.Decode([]byte(tc.body))
			require.ErrorContains(t, err, tc.want)
		})
	}
}

func TestMappingReload(t *testing.T) {
	decoder := configuredDecoder(t, Arguments{Labels: map[string]string{"alertname": ".name"}, StartsAt: ".startsAt"})
	alert, err := decoder.Decode([]byte(`{"name":"First","event":{"type":"Second"},"startsAt":"2026-09-07T14:00:00Z"}`))
	require.NoError(t, err)
	require.Equal(t, model.LabelValue("First"), alert.Labels["alertname"])

	mapping, err := compileMapping(Arguments{Labels: map[string]string{"alertname": ".event.type"}, StartsAt: ".startsAt"})
	require.NoError(t, err)
	decoder.update(mapping)
	alert, err = decoder.Decode([]byte(`{"name":"First","event":{"type":"Second"},"startsAt":"2026-09-07T14:00:00Z"}`))
	require.NoError(t, err)
	require.Equal(t, model.LabelValue("Second"), alert.Labels["alertname"])
}

func TestInvalidPathsFailConfiguration(t *testing.T) {
	for _, path := range []string{"", "labels", ".", ".foo.", ".foo[0]", ".foo?bar"} {
		args := Arguments{Labels: map[string]string{"alertname": path}, StartsAt: ".startsAt"}
		require.Error(t, args.Validate(), path)
	}
}

func configuredDecoder(t *testing.T, args Arguments) *mappingDecoder {
	t.Helper()
	require.NoError(t, args.Validate())
	mapping, err := compileMapping(args)
	require.NoError(t, err)
	decoder := &mappingDecoder{id: "test"}
	decoder.update(mapping)
	return decoder
}
