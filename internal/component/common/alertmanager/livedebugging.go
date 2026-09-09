package alertmanager

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/prometheus/common/model"

	"github.com/grafana/alloy/internal/component"
	"github.com/grafana/alloy/internal/service/livedebugging"
)

// DebugPublisher adapts alert values to Alloy's native lazy debug stream. It owns
// no buffers, subscriptions, or goroutines. Its zero value is disabled.
type DebugPublisher struct {
	publisher livedebugging.DebugDataPublisher
	id        livedebugging.ComponentID
}

// NewDebugPublisher follows loki.source.syslog's optional-service convention.
func NewDebugPublisher(opts component.Options) DebugPublisher {
	d := DebugPublisher{id: livedebugging.ComponentID(opts.ID)}
	if opts.GetServiceData != nil {
		if svc, err := opts.GetServiceData(livedebugging.ServiceName); err == nil {
			d.publisher, _ = svc.(livedebugging.DebugDataPublisher)
		}
	}
	return d
}

// Active reports whether the native service has subscribers.
func (d DebugPublisher) Active() bool { return d.publisher != nil && d.publisher.IsActive(d.id) }

// Publish formats values only when the native consumer requests their text.
func (d DebugPublisher) Publish(count uint64, text func() string) {
	if d.publisher != nil {
		d.publisher.PublishIfActive(livedebugging.NewData(d.id, livedebugging.AlertmanagerAlert, count, text))
	}
}

// Alert exposes the application fields, including the explicit webhook state.
func (d DebugPublisher) Alert(stage string, count uint64, alert Alert) {
	if !d.Active() {
		return
	}
	d.Publish(count, func() string {
		body, _ := json.Marshal(struct {
			Labels       model.LabelSet `json:"labels"`
			Annotations  model.LabelSet `json:"annotations"`
			StartsAt     time.Time      `json:"startsAt"`
			EndsAt       time.Time      `json:"endsAt"`
			GeneratorURL string         `json:"generatorURL"`
			Status       string         `json:"status"`
		}{alert.Labels, alert.Annotations, alert.StartsAt, alert.EndsAt, safeDebugURL(alert.GeneratorURL), string(alert.State)})
		return stage + " " + string(body)
	})
}

// JSON exposes the actual application bytes without parsing or re-encoding them.
func (d DebugPublisher) JSON(stage string, count uint64, body []byte) {
	if !d.Active() {
		return
	}
	d.Publish(count, func() string { return stage + " " + string(body) })
}

// Request exposes logical application headers before client authentication
// wrappers run. Unknown headers are redacted, including custom secret headers.
func (d DebugPublisher) Request(stage string, req *http.Request, body []byte, retry int, configuredHeaders ...string) {
	if !d.Active() {
		return
	}
	d.Publish(0, func() string {
		headers := req.Header
		if len(configuredHeaders) > 0 {
			headers = headers.Clone()
			for _, name := range configuredHeaders {
				headers.Set(name, "***")
			}
		}
		return fmt.Sprintf("%s\n%s %s\nPath: %s\nHeaders: %s\nBody size: %d bytes\nRetry attempt: %d\nBody:\n%s",
			stage, req.Method, safeDebugURL(req.URL.String()), req.URL.EscapedPath(), debugHeaders(headers), len(body), retry, body)
	})
}

func debugHeaders(headers http.Header) string {
	sanitized := make(map[string][]string, len(headers))
	for name, values := range headers {
		// Deny by default: configured secret headers need not have recognizable names.
		switch strings.ToLower(name) {
		case "content-type", "content-length", "accept", "retry-after":
			sanitized[name] = values
		default:
			sanitized[name] = []string{"***"}
		}
	}
	body, _ := json.Marshal(sanitized)
	return string(body)
}

func safeDebugURL(raw string) string {
	u, err := url.Parse(raw)
	if err != nil {
		return "[invalid URL omitted]"
	}
	u.User = nil
	u.Fragment = ""
	// Any query parameter can be a deployment-specific credential. Keep ordinary
	// URLs byte-for-byte, but redact query values without normalizing their keys.
	if u.RawQuery != "" {
		parts := strings.Split(u.RawQuery, "&")
		for i, part := range parts {
			key, _, _ := strings.Cut(part, "=")
			parts[i] = key + "=***"
		}
		u.RawQuery = strings.Join(parts, "&")
	}
	return u.String()
}
