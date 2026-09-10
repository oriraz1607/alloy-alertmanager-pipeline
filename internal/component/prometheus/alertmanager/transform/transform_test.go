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
	transformer.update(tmpl, true, false)

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
	transformer.update(first, false, false)
	body, err := transformer.Transform(testAlert())
	require.NoError(t, err)
	require.JSONEq(t, `{"name":"HighCPU"}`, string(body))

	second, err := compileTemplate(`{"priority": {{ to_json .Labels.severity }}}`)
	require.NoError(t, err)
	transformer.update(second, true, false)
	body, err = transformer.Transform(testAlert())
	require.NoError(t, err)
	require.Equal(t, `{"priority":"critical"}`, string(body))
}

func configuredTransformer(t *testing.T, text string) *templateTransformer {
	t.Helper()
	tmpl, err := compileTemplate(text)
	require.NoError(t, err)
	transformer := &templateTransformer{id: "test"}
	transformer.update(tmpl, false, false)
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

func TestToStringPrefixAndPipeline(t *testing.T) {
	for _, template := range []string{`{"labels":"{{ to_string .Labels }}"}`, `{"labels":"{{ .Labels | to_string }}"}`} {
		alert := testAlert()
		alert.Labels = model.LabelSet{"alertname": "test", "severity": "critical"}
		body, err := configuredTransformer(t, template).Transform(alert)
		require.NoError(t, err)
		require.Equal(t, `{"labels":"{alertname:test,severity:critical}"}`, string(body))
	}
}

func TestRemoveSpecialCharacters(t *testing.T) {
	var args Arguments
	require.NoError(t, syntax.Unmarshal([]byte("template = `{\"labels\":\"{{ to_string .Labels }}\"}`\ncompact = true\nremove_special_characters = true\n"), &args))
	require.True(t, args.RemoveSpecialCharacters)
	var transformer alertpipeline.Transformer
	c, err := New(component.Options{OnStateChange: func(exports component.Exports) { transformer = exports.(Exports).Transformer }}, args)
	require.NoError(t, err)
	alert := testAlert()
	alert.Labels = model.LabelSet{"severity": "critical", "Alert_Name": "Node_Down!"}
	original := alert.Clone()
	body, err := transformer.Transform(alert)
	require.NoError(t, err)
	require.Equal(t, `{"labels":"{AlertName:NodeDown,severity:critical}"}`, string(body))
	require.Equal(t, original, alert)
	var wire map[string]string
	require.NoError(t, json.Unmarshal(body, &wire))
	decoded, err := alertpipeline.LabelsFromString(wire["labels"])
	require.NoError(t, err)
	require.Equal(t, map[string]string{"AlertName": "NodeDown", "severity": "critical"}, decoded)
	args.RemoveSpecialCharacters = false
	require.NoError(t, c.Update(args))
	body, err = transformer.Transform(alert)
	require.NoError(t, err)
	require.Equal(t, `{"labels":"{Alert_Name:Node_Down!,severity:critical}"}`, string(body))
}

func TestCleanLabels(t *testing.T) {
	labels := map[string]string{"custom_name": "a_b!@#$%^&*()-+=[]{}:;,./?\\\"'`~\n\t\x00 😀 שלום 世界 e\u0301 123", "empty": "!!!"}
	cleaned, err := cleanLabels(labels)
	require.NoError(t, err)
	require.Equal(t, map[string]string{"customname": "ab  שלום 世界 e\u0301 123", "empty": ""}, cleaned)
	for _, labels := range []map[string]string{{"!!!": "x"}, {"a_b": "x", "ab": "y"}} {
		_, err := cleanLabels(labels)
		require.Error(t, err)
	}
}
