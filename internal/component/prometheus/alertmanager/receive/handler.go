package receive

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/prometheus/alertmanager/notify/webhook"
	"github.com/prometheus/alertmanager/template"
	"github.com/prometheus/common/model"

	alertpipeline "github.com/grafana/alloy/internal/component/common/alertmanager"
)

func (c *Component) handleWebhook(w http.ResponseWriter, r *http.Request) {
	started := time.Now()
	c.metrics.webhookRequests.Inc()
	c.metrics.activeWebhookRequests.Inc()
	defer func() {
		c.metrics.requestDuration.Observe(time.Since(started).Seconds())
		c.metrics.activeWebhookRequests.Dec()
	}()

	if r.Method != http.MethodPost {
		c.metrics.invalidRequests.Inc()
		w.Header().Set("Allow", http.MethodPost)
		http.Error(w, http.StatusText(http.StatusMethodNotAllowed), http.StatusMethodNotAllowed)
		return
	}

	state := c.getHandlerState()
	message, err := decodeWebhook(w, r, state.maxRequestBodySize)
	if err != nil {
		c.metrics.invalidRequests.Inc()
		statusCode := http.StatusBadRequest
		var maxBytesErr *http.MaxBytesError
		if errors.As(err, &maxBytesErr) {
			statusCode = http.StatusRequestEntityTooLarge
		}
		c.opts.Logger.Warn("rejected Alertmanager webhook", "err", err, "status_code", statusCode)
		http.Error(w, http.StatusText(statusCode), statusCode)
		return
	}
	if message.Data == nil || len(message.Alerts) == 0 {
		c.metrics.invalidRequests.Inc()
		c.opts.Logger.Warn("rejected Alertmanager webhook without alerts")
		http.Error(w, "webhook must contain at least one alert", http.StatusBadRequest)
		return
	}

	c.debugDataPublisher.Publish(0, func() string {
		body, _ := json.Marshal(message)
		return "[IN] WEBHOOK " + string(body)
	})
	alerts, err := convertAlerts(message.Alerts)
	if err != nil {
		c.metrics.invalidRequests.Inc()
		c.opts.Logger.Warn("rejected invalid alert data", "err", err)
		http.Error(w, "webhook contains invalid alert data", http.StatusBadRequest)
		return
	}
	for _, alert := range alerts {
		c.debugDataPublisher.Alert("[OUT]", 1, alert)
	}
	c.metrics.receivedAlerts.Add(float64(len(alerts)))

	forwardCtx, cancel := context.WithTimeout(r.Context(), state.forwardTimeout)
	defer cancel()
	if err := c.fanout.Send(forwardCtx, alerts); err != nil {
		c.metrics.forwardingFailures.Inc()
		c.opts.Logger.Warn("downstream receiver rejected Alertmanager webhook", "err", err)
		statusCode := http.StatusServiceUnavailable
		if errors.Is(err, context.DeadlineExceeded) || errors.Is(forwardCtx.Err(), context.DeadlineExceeded) {
			statusCode = http.StatusGatewayTimeout
		}
		http.Error(w, "downstream receiver did not accept alerts", statusCode)
		return
	}

	c.metrics.forwardedAlerts.Add(float64(len(alerts)))
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok\n"))
}

func decodeWebhook(w http.ResponseWriter, r *http.Request, maxBodySize int64) (*webhook.Message, error) {
	body := http.MaxBytesReader(w, r.Body, maxBodySize)
	defer body.Close()

	decoder := json.NewDecoder(body)
	var message webhook.Message
	if err := decoder.Decode(&message); err != nil {
		return nil, fmt.Errorf("decoding webhook JSON: %w", err)
	}
	if err := ensureJSONEOF(decoder); err != nil {
		return nil, err
	}
	return &message, nil
}

func ensureJSONEOF(decoder *json.Decoder) error {
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		if err == nil {
			return fmt.Errorf("webhook body must contain exactly one JSON value")
		}
		return fmt.Errorf("decoding trailing webhook data: %w", err)
	}
	return nil
}

func convertAlerts(in template.Alerts) ([]alertpipeline.Alert, error) {
	out := make([]alertpipeline.Alert, 0, len(in))
	for i, incoming := range in {
		alert := alertpipeline.Alert{
			Alert: model.Alert{
				Labels:       make(model.LabelSet, len(incoming.Labels)),
				Annotations:  make(model.LabelSet, len(incoming.Annotations)),
				StartsAt:     incoming.StartsAt,
				EndsAt:       incoming.EndsAt,
				GeneratorURL: incoming.GeneratorURL,
			},
			State:             model.AlertStatus(incoming.Status),
			SourceFingerprint: incoming.Fingerprint,
		}
		for name, value := range incoming.Labels {
			alert.Labels[model.LabelName(name)] = model.LabelValue(value)
		}
		for name, value := range incoming.Annotations {
			alert.Annotations[model.LabelName(name)] = model.LabelValue(value)
		}
		if err := alert.Validate(); err != nil {
			return nil, fmt.Errorf("alert %d is invalid: %w", i, err)
		}
		out = append(out, alert)
	}
	return out, nil
}
