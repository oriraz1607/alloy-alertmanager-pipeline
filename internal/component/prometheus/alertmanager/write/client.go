package write

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/prometheus/alertmanager/api/v2/models"
	"github.com/prometheus/common/model"

	alertpipeline "github.com/grafana/alloy/internal/component/common/alertmanager"
)

func (c *Component) sendWithRetry(ctx context.Context, alerts []alertpipeline.Alert, preserve bool) error {
	retries := 0
	for {
		state := c.getDeliveryState()
		err := c.sendOnce(ctx, state, alerts)
		if err == nil {
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		failure, ok := err.(*alertpipeline.HTTPFailure)
		if !ok || !alertpipeline.IsRetryableHTTP(failure, state.retryOnHTTP429) {
			return err
		}
		if !preserve && state.maxRetries > 0 && retries >= state.maxRetries {
			return err
		}

		delay := alertpipeline.HTTPRetryDelay(state.minBackoff, state.maxBackoff, retries)
		retries++
		c.metrics.retries.Inc()
		c.opts.Logger.Warn("retrying Alertmanager request", "reason", failure.Reason, "retry", retries, "backoff", delay)
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			stopTimer(timer)
			return ctx.Err()
		case <-timer.C:
		}
	}
}

func (c *Component) sendOnce(ctx context.Context, state deliveryState, alerts []alertpipeline.Alert) error {
	body, err := marshalAlerts(alerts, time.Now(), state.firingAlertDuration)
	if err != nil {
		return &alertpipeline.HTTPFailure{Reason: alertpipeline.HTTPFailureStatusOther, Err: fmt.Errorf("encoding alerts: %w", err)}
	}

	c.debugDataPublisher.JSON("[OUT] ALERTMANAGER REQUEST", 0, body)
	c.metrics.httpRequests.Inc()
	duration, err := alertpipeline.PostJSON(ctx, state.client, state.endpointURL, body, nil, state.timeout)
	c.metrics.httpRequestDuration.Observe(duration.Seconds())
	if err != nil {
		if failure, ok := err.(*alertpipeline.HTTPFailure); ok {
			c.metrics.httpRequestFailures.WithLabelValues(string(failure.Reason)).Inc()
		}
		return err
	}
	return nil
}

func marshalAlerts(alerts []alertpipeline.Alert, now time.Time, firingAlertDuration time.Duration) ([]byte, error) {
	postable := make(models.PostableAlerts, 0, len(alerts))
	for i, alert := range alerts {
		endsAt := alert.EndsAt
		if alert.State == model.AlertFiring {
			endsAt = now.Add(firingAlertDuration)
		} else if endsAt.IsZero() || endsAt.After(now) {
			// The API derives firing/resolved state from EndsAt. An explicit resolved
			// pipeline value must therefore never be encoded with a future end time.
			endsAt = now
		}

		labels := make(models.LabelSet, len(alert.Labels))
		for name, value := range alert.Labels {
			labels[string(name)] = string(value)
		}
		annotations := make(models.LabelSet, len(alert.Annotations))
		for name, value := range alert.Annotations {
			annotations[string(name)] = string(value)
		}
		out := &models.PostableAlert{
			Annotations: annotations,
			StartsAt:    strfmt.DateTime(alert.StartsAt),
			EndsAt:      strfmt.DateTime(endsAt),
			Alert: models.Alert{
				GeneratorURL: strfmt.URI(alert.GeneratorURL),
				Labels:       labels,
			},
		}
		if err := out.Validate(strfmt.Default); err != nil {
			return nil, fmt.Errorf("alert %d is invalid: %w", i, err)
		}
		postable = append(postable, out)
	}
	return json.Marshal(postable)
}
