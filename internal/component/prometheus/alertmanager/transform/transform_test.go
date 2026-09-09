package transform

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/prometheus/common/model"
	"github.com/stretchr/testify/require"

	"github.com/grafana/alloy/internal/component"
	alertpipeline "github.com/grafana/alloy/internal/component/common/alertmanager"
	"github.com/grafana/alloy/syntax"
)

func TestRegistrationAndConfiguration(t *testing.T) {
	registration, found := component.Get("prometheus.alertmanager.transform")
	require.True(t, found)
	require.True(t, registration.Community)

	var args Arguments
	require.NoError(t, syntax.Unmarshal([]byte("template = \"{\\\"name\\\": {{ to_json .Labels.alertname }}}\"\ncompact = true"), &args))
	require.Contains(t, args.Template, ".Labels.alertname")
	require.True(t, args.Compact)
}

func TestTemplateTransformsWholeMapsFieldsAndEscaping(t *testing.T) {
	transformer := configuredTransformer(t, `{
      "schema_version": 1,
      "event": {"name": {{ to_json .Labels.alertname }}, "priority": {{ to_json .Labels.severity }}},
      "labels": {{ to_json .Labels }},
      "annotations": {{ to_json .Annotations }},
      "message": {{ to_json .Annotations.summary }},
      "starts_at": {{ to_json .StartsAt }},
      "ends_at": {{ to_json .EndsAt }},
      "status": {{ to_json .Status }}
    }`)

	alert := testAlert()
	body, err := transformer.Transform(alert)
	require.NoError(t, err)
	var got map[string]any
	require.NoError(t, json.Unmarshal(body, &got))
	require.Equal(t, float64(1), got["schema_version"])
	require.Equal(t, "HighCPU", got["event"].(map[string]any)["name"])
	require.Equal(t, "critical", got["event"].(map[string]any)["priority"])
	require.Equal(t, "production", got["labels"].(map[string]any)["cluster"])
	require.Equal(t, "monitoring", got["labels"].(map[string]any)["namespace"])
	require.Equal(t, "quoted \"line\"\nשלום", got["message"])
	require.Equal(t, "runbook-value", got["annotations"].(map[string]any)["new_annotation"])
	require.Equal(t, "2026-09-07T14:00:00Z", got["starts_at"])
	require.Equal(t, "firing", got["status"])
}

func TestDifferentTemplatesAndOmittedFields(t *testing.T) {
	alert := testAlert()
	first, err := configuredTransformer(t, `{"name": {{ to_json .Labels.alertname }}}`).Transform(alert)
	require.NoError(t, err)
	second, err := configuredTransformer(t, `{"event": {"priority": {{ to_json .Labels.severity }}}}`).Transform(alert)
	require.NoError(t, err)
	require.JSONEq(t, `{"name":"HighCPU"}`, string(first))
	require.JSONEq(t, `{"event":{"priority":"critical"}}`, string(second))
	require.NotEqual(t, first, second)
}

func TestCompactJSON(t *testing.T) {
	tmpl, err := compileTemplate(`{
  "labels": {
    "severity": {{ to_json .Labels.severity }}
  },
  "message": "spaces inside strings stay here"
}`)
	require.NoError(t, err)
	transformer := &templateTransformer{id: "compact"}
	transformer.update(tmpl, true)

	body, err := transformer.Transform(testAlert())
	require.NoError(t, err)
	require.Equal(t, `{"labels":{"severity":"critical"},"message":"spaces inside strings stay here"}`, string(body))
}

func TestMissingDefaultAndRequired(t *testing.T) {
	alert := testAlert()
	body, err := configuredTransformer(t, `{"severity": {{ to_json (default "unknown" .Labels.missing) }}}`).Transform(alert)
	require.NoError(t, err)
	require.JSONEq(t, `{"severity":"unknown"}`, string(body))

	_, err = configuredTransformer(t, `{"severity": {{ to_json (required "severity is required" .Labels.missing) }}}`).Transform(alert)
	require.ErrorContains(t, err, "severity is required")
}

func TestTemplateAndRenderedJSONErrors(t *testing.T) {
	_, err := compileTemplate(`{"name": {{`)
	require.Error(t, err)
	_, err = configuredTransformer(t, `{"name": {{ to_json .Labels.alertname }}`).Transform(testAlert())
	require.ErrorContains(t, err, "not valid JSON")
}

func TestTemplateReloadChangesSubsequentOutput(t *testing.T) {
	transformer := &templateTransformer{id: "reload"}
	first, err := compileTemplate(`{"name": {{ to_json .Labels.alertname }}}`)
	require.NoError(t, err)
	transformer.update(first, false)
	body, err := transformer.Transform(testAlert())
	require.NoError(t, err)
	require.JSONEq(t, `{"name":"HighCPU"}`, string(body))

	second, err := compileTemplate(`{"priority": {{ to_json .Labels.severity }}}`)
	require.NoError(t, err)
	transformer.update(second, true)
	body, err = transformer.Transform(testAlert())
	require.NoError(t, err)
	require.Equal(t, `{"priority":"critical"}`, string(body))
}

func configuredTransformer(t *testing.T, text string) *templateTransformer {
	t.Helper()
	tmpl, err := compileTemplate(text)
	require.NoError(t, err)
	transformer := &templateTransformer{id: "test"}
	transformer.update(tmpl, false)
	return transformer
}

func testAlert() alertpipeline.Alert {
	return alertpipeline.Alert{
		Alert: model.Alert{
			Labels: model.LabelSet{
				"alertname": "HighCPU",
				"severity":  "critical",
				"instance":  "server01",
				"cluster":   "production",
				"namespace": "monitoring",
			},
			Annotations: model.LabelSet{
				"summary":        "quoted \"line\"\nשלום",
				"new_annotation": "runbook-value",
			},
			StartsAt:     time.Date(2026, 9, 7, 14, 0, 0, 0, time.UTC),
			EndsAt:       time.Date(2026, 9, 7, 15, 0, 0, 0, time.UTC),
			GeneratorURL: "https://example.test/graph",
		},
		State:             model.AlertFiring,
		SourceFingerprint: "source",
	}
}
