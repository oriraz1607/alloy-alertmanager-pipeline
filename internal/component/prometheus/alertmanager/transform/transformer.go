package transform

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log/slog"
	"reflect"
	"sync"
	"text/template"
	"time"

	"github.com/prometheus/common/model"

	alertpipeline "github.com/grafana/alloy/internal/component/common/alertmanager"
)

type templateContext struct {
	Labels            map[string]string
	Annotations       map[string]string
	StartsAt          time.Time
	EndsAt            time.Time
	GeneratorURL      string
	Status            string
	SourceFingerprint string
}

type templateTransformer struct {
	debugDataPublisher alertpipeline.DebugPublisher
	logger             *slog.Logger
	id                 string
	mut                sync.RWMutex
	template           *template.Template
	compact            bool
}

func (t *templateTransformer) String() string { return t.id + ".transformer" }

func (t *templateTransformer) Transform(alert alertpipeline.Alert) (output []byte, err error) {
	t.debugDataPublisher.Alert("[IN]", 0, alert)
	defer func() {
		if err != nil && t.logger != nil {
			t.logger.Warn("failed to transform alert JSON", "err", err)
		}
	}()
	t.mut.RLock()
	tmpl := t.template
	compact := t.compact
	t.mut.RUnlock()
	if tmpl == nil {
		return nil, fmt.Errorf("transformer is not configured")
	}
	ctx := templateContext{
		Labels:            labelMap(alert.Labels),
		Annotations:       labelMap(alert.Annotations),
		StartsAt:          alert.StartsAt,
		EndsAt:            alert.EndsAt,
		GeneratorURL:      alert.GeneratorURL,
		Status:            string(alert.State),
		SourceFingerprint: alert.SourceFingerprint,
	}
	var rendered bytes.Buffer
	if err := tmpl.Execute(&rendered, ctx); err != nil {
		return nil, fmt.Errorf("rendering alert JSON template: %w", err)
	}
	body := bytes.TrimSpace(rendered.Bytes())
	if !json.Valid(body) {
		return nil, fmt.Errorf("rendered alert template is not valid JSON")
	}
	if compact {
		var compacted bytes.Buffer
		if err := json.Compact(&compacted, body); err != nil {
			return nil, fmt.Errorf("compacting rendered alert template: %w", err)
		}
		body = compacted.Bytes()
	}
	t.debugDataPublisher.JSON("[OUT]", 1, body)
	return bytes.Clone(body), nil
}

func (t *templateTransformer) update(tmpl *template.Template, compact bool) {
	t.mut.Lock()
	t.template = tmpl
	t.compact = compact
	t.mut.Unlock()
}

func compileTemplate(text string) (*template.Template, error) {
	return template.New("alert_json").Funcs(template.FuncMap{
		"to_json":   toJSON,
		"to_string": alertpipeline.LabelsToString,
		"default":   defaultValue,
		"required":  requiredValue,
	}).Option("missingkey=zero").Parse(text)
}

func toJSON(value any) (string, error) {
	encoded, err := json.Marshal(value)
	if err != nil {
		return "", err
	}
	return string(encoded), nil
}

func defaultValue(fallback, value any) any {
	if isEmpty(value) {
		return fallback
	}
	return value
}

func requiredValue(message string, value any) (any, error) {
	if isEmpty(value) {
		return nil, fmt.Errorf("%s", message)
	}
	return value, nil
}

func isEmpty(value any) bool {
	if value == nil {
		return true
	}
	v := reflect.ValueOf(value)
	switch v.Kind() {
	case reflect.Array, reflect.Map, reflect.Slice, reflect.String:
		return v.Len() == 0
	case reflect.Bool:
		return !v.Bool()
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return v.Int() == 0
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		return v.Uint() == 0
	case reflect.Float32, reflect.Float64:
		return v.Float() == 0
	case reflect.Interface, reflect.Pointer:
		return v.IsNil()
	default:
		return false
	}
}

func labelMap(labels model.LabelSet) map[string]string {
	result := make(map[string]string, len(labels))
	for name, value := range labels {
		result[string(name)] = string(value)
	}
	return result
}
