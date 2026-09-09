package decode

import (
	"fmt"
	"strings"
	"unicode"

	"github.com/prometheus/common/model"
)

// Arguments configures prometheus.alertmanager.decode.
type Arguments struct {
	LabelsFrom        string            `alloy:"labels_from,attr,optional"`
	AnnotationsFrom   string            `alloy:"annotations_from,attr,optional"`
	Labels            map[string]string `alloy:"labels,attr,optional"`
	Annotations       map[string]string `alloy:"annotations,attr,optional"`
	StartsAt          string            `alloy:"starts_at,attr,optional"`
	EndsAt            string            `alloy:"ends_at,attr,optional"`
	GeneratorURL      string            `alloy:"generator_url,attr,optional"`
	Status            string            `alloy:"status,attr,optional"`
	SourceFingerprint string            `alloy:"source_fingerprint,attr,optional"`
}

// Validate implements syntax.Validator.
func (args *Arguments) Validate() error {
	paths := map[string]string{
		"labels_from":        args.LabelsFrom,
		"annotations_from":   args.AnnotationsFrom,
		"starts_at":          args.StartsAt,
		"ends_at":            args.EndsAt,
		"generator_url":      args.GeneratorURL,
		"status":             args.Status,
		"source_fingerprint": args.SourceFingerprint,
	}
	for name, value := range paths {
		if value != "" {
			if _, err := parsePath(value); err != nil {
				return fmt.Errorf("invalid %s: %w", name, err)
			}
		}
	}
	for name, value := range args.Labels {
		if !model.LegacyValidation.IsValidLabelName(name) {
			return fmt.Errorf("invalid label name %q", name)
		}
		if _, err := parsePath(value); err != nil {
			return fmt.Errorf("invalid path for label %q: %w", name, err)
		}
	}
	for name, value := range args.Annotations {
		if !model.LegacyValidation.IsValidLabelName(name) {
			return fmt.Errorf("invalid annotation name %q", name)
		}
		if _, err := parsePath(value); err != nil {
			return fmt.Errorf("invalid path for annotation %q: %w", name, err)
		}
	}
	if args.LabelsFrom == "" && len(args.Labels) == 0 {
		return fmt.Errorf("at least one of labels_from or labels must be configured")
	}
	if args.StartsAt == "" {
		return fmt.Errorf("starts_at must be configured because typed alerts require a start time")
	}
	return nil
}

type fieldPath struct {
	raw      string
	segments []string
}

func parsePath(raw string) (fieldPath, error) {
	if raw == "" || raw[0] != '.' || len(raw) == 1 || strings.HasSuffix(raw, ".") {
		return fieldPath{}, fmt.Errorf("path %q must use .field syntax", raw)
	}
	segments := strings.Split(raw[1:], ".")
	for _, segment := range segments {
		if segment == "" {
			return fieldPath{}, fmt.Errorf("path %q contains an empty segment", raw)
		}
		for _, r := range segment {
			if !(unicode.IsLetter(r) || unicode.IsDigit(r) || r == '_' || r == '-') {
				return fieldPath{}, fmt.Errorf("path %q contains unsupported character %q", raw, r)
			}
		}
	}
	return fieldPath{raw: raw, segments: segments}, nil
}
