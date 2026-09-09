package http_receive

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	alertpipeline "github.com/grafana/alloy/internal/component/common/alertmanager"
)

func (c *Component) handleRequest(w http.ResponseWriter, r *http.Request) {
	started := time.Now()
	c.metrics.requests.Inc()
	c.metrics.activeRequests.Inc()
	defer func() {
		c.metrics.requestDuration.Observe(time.Since(started).Seconds())
		c.metrics.activeRequests.Dec()
	}()

	state := c.getHandlerState()
	if r.URL.Path != state.path {
		c.metrics.invalidRequests.WithLabelValues("path").Inc()
		http.NotFound(w, r)
		return
	}
	if r.Method != http.MethodPost {
		c.metrics.invalidRequests.WithLabelValues("method").Inc()
		w.Header().Set("Allow", http.MethodPost)
		http.Error(w, http.StatusText(http.StatusMethodNotAllowed), http.StatusMethodNotAllowed)
		return
	}

	bodyReader := http.MaxBytesReader(w, r.Body, state.maxRequestBodySize)
	body, err := io.ReadAll(bodyReader)
	_ = bodyReader.Close()
	if err != nil {
		statusCode := http.StatusBadRequest
		var maxBytesErr *http.MaxBytesError
		if errors.As(err, &maxBytesErr) {
			statusCode = http.StatusRequestEntityTooLarge
		}
		c.metrics.invalidRequests.WithLabelValues("body").Inc()
		c.opts.Logger.Warn("rejected custom alert request body", "err", err, "status_code", statusCode)
		http.Error(w, http.StatusText(statusCode), statusCode)
		return
	}
	c.debugDataPublisher.Request("[IN] HTTP RECEIVED", r, body, 0)
	alert, err := state.decoder.Decode(body)
	if err != nil {
		c.metrics.invalidRequests.WithLabelValues("decoder").Inc()
		c.opts.Logger.Warn("rejected custom alert JSON", "err", err)
		http.Error(w, "request body doesn't match the configured alert schema", http.StatusBadRequest)
		return
	}
	if err := alert.Validate(); err != nil {
		c.metrics.invalidRequests.WithLabelValues("alert").Inc()
		c.opts.Logger.Warn("decoder returned an invalid alert", "err", err)
		http.Error(w, "decoder returned invalid alert data", http.StatusBadRequest)
		return
	}
	c.debugDataPublisher.Alert("[OUT]", 1, alert)
	c.metrics.receivedAlerts.Inc()

	forwardCtx, cancel := context.WithTimeout(r.Context(), state.forwardTimeout)
	defer cancel()
	if err := c.fanout.Send(forwardCtx, []alertpipeline.Alert{alert}); err != nil {
		c.metrics.forwardingFailures.Inc()
		c.opts.Logger.Warn("downstream receiver rejected decoded alert", "err", err)
		statusCode := http.StatusServiceUnavailable
		if errors.Is(err, context.DeadlineExceeded) || errors.Is(forwardCtx.Err(), context.DeadlineExceeded) {
			statusCode = http.StatusGatewayTimeout
		}
		http.Error(w, "downstream receiver did not accept decoded alert", statusCode)
		return
	}
	c.metrics.forwardedAlerts.Inc()
	w.WriteHeader(http.StatusOK)
	_, _ = fmt.Fprintln(w, "ok")
}
