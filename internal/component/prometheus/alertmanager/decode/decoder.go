package decode

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/prometheus/common/model"

	alertpipeline "github.com/grafana/alloy/internal/component/common/alertmanager"
)

type compiledMapping struct {
	labelsFormat      string
	labelsFrom        *fieldPath
	annotationsFrom   *fieldPath
	labels            map[string]fieldPath
	annotations       map[string]fieldPath
	startsAt          *fieldPath
	endsAt            *fieldPath
	generatorURL      *fieldPath
	status            *fieldPath
	sourceFingerprint *fieldPath
}

type mappingDecoder struct {
	debugDataPublisher alertpipeline.DebugPublisher
	id                 string
	mut                sync.RWMutex
	mapping            *compiledMapping
}

func (d *mappingDecoder) String() string { return d.id + ".decoder" }

func (d *mappingDecoder) Decode(body []byte) (alertpipeline.Alert, error) {
	d.debugDataPublisher.JSON("[IN]", 0, body)
	d.mut.RLock()
	mapping := d.mapping
	d.mut.RUnlock()
	if mapping == nil {
		return alertpipeline.Alert{}, fmt.Errorf("decoder is not configured")
	}
	root, err := decodeObject(body)
	if err != nil {
		return alertpipeline.Alert{}, err
	}

	alert := alertpipeline.Alert{Alert: model.Alert{
		Labels:      model.LabelSet{},
		Annotations: model.LabelSet{},
	}}
	if err := importLabels(root, mapping, alert.Labels); err != nil {
		return alertpipeline.Alert{}, err
	}
	if err := importStringMap(root, mapping.annotationsFrom, "annotations_from", alert.Annotations); err != nil {
		return alertpipeline.Alert{}, err
	}
	if err := applyStringMappings(root, mapping.labels, "label", alert.Labels); err != nil {
		return alertpipeline.Alert{}, err
	}
	if err := applyStringMappings(root, mapping.annotations, "annotation", alert.Annotations); err != nil {
		return alertpipeline.Alert{}, err
	}
	if alert.StartsAt, err = readTime(root, mapping.startsAt, "starts_at"); err != nil {
		return alertpipeline.Alert{}, err
	}
	if alert.EndsAt, err = readTime(root, mapping.endsAt, "ends_at"); err != nil {
		return alertpipeline.Alert{}, err
	}
	if alert.GeneratorURL, err = readString(root, mapping.generatorURL, "generator_url"); err != nil {
		return alertpipeline.Alert{}, err
	}
	if alert.SourceFingerprint, err = readString(root, mapping.sourceFingerprint, "source_fingerprint"); err != nil {
		return alertpipeline.Alert{}, err
	}
	if mapping.status != nil {
		status, err := readString(root, mapping.status, "status")
		if err != nil {
			return alertpipeline.Alert{}, err
		}
		alert.State = model.AlertStatus(status)
	} else if !alert.EndsAt.IsZero() && !alert.EndsAt.After(time.Now()) {
		alert.State = model.AlertResolved
	} else {
		alert.State = model.AlertFiring
	}
	if err := alert.Validate(); err != nil {
		return alertpipeline.Alert{}, fmt.Errorf("decoded alert is invalid: %w", err)
	}
	d.debugDataPublisher.Alert("[OUT]", 1, alert)
	return alert, nil
}

func (d *mappingDecoder) update(mapping *compiledMapping) {
	d.mut.Lock()
	d.mapping = mapping
	d.mut.Unlock()
}

func compileMapping(args Arguments) (*compiledMapping, error) {
	result := &compiledMapping{
		labelsFormat: args.LabelsFormat,
		labels:       make(map[string]fieldPath, len(args.Labels)),
		annotations:  make(map[string]fieldPath, len(args.Annotations)),
	}
	var err error
	if result.labelsFrom, err = optionalPath(args.LabelsFrom); err != nil {
		return nil, err
	}
	if result.annotationsFrom, err = optionalPath(args.AnnotationsFrom); err != nil {
		return nil, err
	}
	if result.startsAt, err = optionalPath(args.StartsAt); err != nil {
		return nil, err
	}
	if result.endsAt, err = optionalPath(args.EndsAt); err != nil {
		return nil, err
	}
	if result.generatorURL, err = optionalPath(args.GeneratorURL); err != nil {
		return nil, err
	}
	if result.status, err = optionalPath(args.Status); err != nil {
		return nil, err
	}
	if result.sourceFingerprint, err = optionalPath(args.SourceFingerprint); err != nil {
		return nil, err
	}
	for name, raw := range args.Labels {
		result.labels[name], err = parsePath(raw)
		if err != nil {
			return nil, err
		}
	}
	for name, raw := range args.Annotations {
		result.annotations[name], err = parsePath(raw)
		if err != nil {
			return nil, err
		}
	}
	return result, nil
}

func optionalPath(raw string) (*fieldPath, error) {
	if raw == "" {
		return nil, nil
	}
	path, err := parsePath(raw)
	return &path, err
}

func decodeObject(body []byte) (map[string]any, error) {
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.UseNumber()
	var root map[string]any
	if err := decoder.Decode(&root); err != nil {
		return nil, fmt.Errorf("decoding JSON object: %w", err)
	}
	if root == nil {
		return nil, fmt.Errorf("request body must contain one JSON object")
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		if err == nil {
			return nil, fmt.Errorf("request body must contain exactly one JSON value")
		}
		return nil, fmt.Errorf("decoding trailing JSON data: %w", err)
	}
	return root, nil
}

func resolve(root map[string]any, path *fieldPath) (any, bool, error) {
	if path == nil {
		return nil, false, nil
	}
	var current any = root
	for index, segment := range path.segments {
		object, ok := current.(map[string]any)
		if !ok {
			return nil, false, fmt.Errorf("path %s traverses a non-object before %q", path.raw, segment)
		}
		current, ok = object[segment]
		if !ok {
			return nil, false, nil
		}
		if index == len(path.segments)-1 {
			return current, true, nil
		}
	}
	return nil, false, nil
}

func importStringMap(root map[string]any, path *fieldPath, name string, target model.LabelSet) error {
	value, found, err := resolve(root, path)
	if err != nil || !found {
		return err
	}
	object, ok := value.(map[string]any)
	if !ok {
		return fmt.Errorf("%s path %s must select a JSON object", name, path.raw)
	}
	for key, rawValue := range object {
		if !model.LegacyValidation.IsValidLabelName(key) {
			return fmt.Errorf("%s path %s contains invalid name %q", name, path.raw, key)
		}
		value, ok := rawValue.(string)
		if !ok {
			return fmt.Errorf("%s path %s key %q must contain a JSON string", name, path.raw, key)
		}
		target[model.LabelName(key)] = model.LabelValue(value)
	}
	return nil
}

func applyStringMappings(root map[string]any, mappings map[string]fieldPath, kind string, target model.LabelSet) error {
	for name, path := range mappings {
		value, found, err := resolve(root, &path)
		if err != nil {
			return err
		}
		if !found {
			return fmt.Errorf("required %s %q path %s is missing", kind, name, path.raw)
		}
		text, ok := value.(string)
		if !ok {
			return fmt.Errorf("%s %q path %s must select a JSON string", kind, name, path.raw)
		}
		target[model.LabelName(name)] = model.LabelValue(text)
	}
	return nil
}

func readString(root map[string]any, path *fieldPath, name string) (string, error) {
	if path == nil {
		return "", nil
	}
	value, found, err := resolve(root, path)
	if err != nil {
		return "", err
	}
	if !found {
		return "", fmt.Errorf("required %s path %s is missing", name, path.raw)
	}
	text, ok := value.(string)
	if !ok {
		return "", fmt.Errorf("%s path %s must select a JSON string", name, path.raw)
	}
	return text, nil
}

func readTime(root map[string]any, path *fieldPath, name string) (time.Time, error) {
	text, err := readString(root, path, name)
	if err != nil || path == nil {
		return time.Time{}, err
	}
	value, err := time.Parse(time.RFC3339Nano, text)
	if err != nil {
		return time.Time{}, fmt.Errorf("%s path %s must contain an RFC3339 timestamp: %w", name, path.raw, err)
	}
	return value, nil
}

func importLabels(root map[string]any, mapping *compiledMapping, target model.LabelSet) error {
	if mapping.labelsFormat != "to_string" {
		return importStringMap(root, mapping.labelsFrom, "labels_from", target)
	}
	value, found, err := resolve(root, mapping.labelsFrom)
	if err != nil || !found {
		return err
	}
	text, ok := value.(string)
	if !ok {
		return fmt.Errorf("labels_from path %s must select a JSON string", mapping.labelsFrom.raw)
	}
	labels, err := alertpipeline.LabelsFromString(text)
	if err != nil {
		return fmt.Errorf("labels_from path %s: %w", mapping.labelsFrom.raw, err)
	}
	for key, value := range labels {
		if !model.LegacyValidation.IsValidLabelName(key) {
			return fmt.Errorf("labels_from path %s contains invalid name %q", mapping.labelsFrom.raw, key)
		}
		target[model.LabelName(key)] = model.LabelValue(value)
	}
	return nil
}
