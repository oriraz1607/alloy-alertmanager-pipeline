package http

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	stdhttp "net/http"
	"sync"
	"time"

	promconfig "github.com/prometheus/common/config"

	"github.com/grafana/alloy/internal/component"
	alertpipeline "github.com/grafana/alloy/internal/component/common/alertmanager"
	"github.com/grafana/alloy/internal/useragent"
)

func init() {
	component.Register(component.Registration{
		Name:      "prometheus.alertmanager.http",
		Community: true,
		Args:      Arguments{},
		Exports:   Exports{},
		Build: func(opts component.Options, args component.Arguments) (component.Component, error) {
			return New(opts, args.(Arguments))
		},
	})
}

// Exports holds the typed receiver used by upstream alert components.
type Exports struct {
	Receiver alertpipeline.Receiver `alloy:"receiver,attr"`
}

type deliveryState struct {
	omitDebugResponseBody bool
	debugHeaderNames      []string
	transformer           alertpipeline.Transformer
	client                *stdhttp.Client
	endpointURL           string
	timeout               time.Duration
	minBackoff            time.Duration
	maxBackoff            time.Duration
	maxRetries            int
	retryOnHTTP429        bool
	drainTimeout          time.Duration
	blockOverflow         bool
}

type receiver struct {
	component *Component
	id        string
}

func (r *receiver) Send(ctx context.Context, alerts []alertpipeline.Alert) error {
	return r.component.receive(ctx, alerts)
}
func (r *receiver) String() string { return r.id + ".receiver" }

// Component transforms typed alerts and reliably sends one JSON request per alert.
type Component struct {
	debugDataPublisher alertpipeline.DebugPublisher
	opts               component.Options
	metrics            *metrics
	queue              *payloadQueue
	receiver           alertpipeline.Receiver
	receiveGate        chan struct{}

	stateMut sync.RWMutex
	state    deliveryState

	healthMut sync.RWMutex
	health    component.Health
}

var (
	_ component.LiveDebugging   = (*Component)(nil)
	_ component.Component       = (*Component)(nil)
	_ component.HealthComponent = (*Component)(nil)
)

// New creates a prometheus.alertmanager.http component.
func New(opts component.Options, args Arguments) (*Component, error) {
	if err := args.Validate(); err != nil {
		return nil, err
	}
	c := &Component{
		debugDataPublisher: alertpipeline.NewDebugPublisher(opts),
		opts:               opts,
		metrics:            newMetrics(opts.Registerer),
		queue:              newPayloadQueue(args.Queue.Capacity),
		receiveGate:        make(chan struct{}, 1),
		health:             component.Health{Health: component.HealthTypeUnknown, Message: "component is starting", UpdateTime: time.Now()},
	}
	c.receiveGate <- struct{}{}
	c.receiver = &receiver{component: c, id: opts.ID}
	if err := c.Update(args); err != nil {
		return nil, err
	}
	opts.OnStateChange(Exports{Receiver: c.receiver})
	return c, nil
}

// Run delivers queued JSON bodies and drains them during shutdown.
func (c *Component) Run(ctx context.Context) error {
	c.setHealth(component.HealthTypeHealthy, "component is ready to accept alerts")
	for {
		item, wait, ready := c.queue.takeReady(time.Now())
		if ready {
			if c.attemptDelivery(ctx, item) {
				c.queue.retry(item, time.Time{})
				c.updateQueueMetric()
				return c.drain()
			}
			continue
		}
		if !c.waitForQueue(ctx, wait) {
			return c.drain()
		}
	}
}

// Update applies transformer, endpoint, retry, and queue settings.
func (c *Component) Update(args component.Arguments) error {
	newArgs := args.(Arguments)
	if err := newArgs.Validate(); err != nil {
		return err
	}
	client, err := promconfig.NewClientFromConfig(*newArgs.Endpoint.HTTPClientConfig.Convert(), c.opts.ID, promconfig.WithUserAgent(useragent.Get()))
	if err != nil {
		return fmt.Errorf("creating destination HTTP client: %w", err)
	}
	cfg := newArgs.Endpoint.HTTPClientConfig
	state := deliveryState{
		omitDebugResponseBody: cfg.BasicAuth != nil || cfg.Authorization != nil || cfg.OAuth2 != nil || cfg.BearerToken != "" || cfg.BearerTokenFile != "" || cfg.HTTPHeaders != nil,
		transformer:           newArgs.Transformer,
		client:                client,
		endpointURL:           newArgs.Endpoint.URL,
		timeout:               newArgs.Endpoint.Timeout,
		minBackoff:            newArgs.Endpoint.MinBackoff,
		maxBackoff:            newArgs.Endpoint.MaxBackoff,
		maxRetries:            newArgs.Endpoint.MaxRetries,
		retryOnHTTP429:        newArgs.Endpoint.RetryOnHTTP429,
		drainTimeout:          newArgs.Queue.DrainTimeout,
		blockOverflow:         newArgs.Queue.BlockOnOverflow,
	}
	if cfg.BasicAuth != nil || cfg.Authorization != nil || cfg.OAuth2 != nil || cfg.BearerToken != "" || cfg.BearerTokenFile != "" {
		state.debugHeaderNames = append(state.debugHeaderNames, "Authorization")
	}
	if cfg.ProxyConfig != nil {
		state.omitDebugResponseBody = state.omitDebugResponseBody || cfg.ProxyConfig.ProxyFromEnvironment || cfg.ProxyConfig.ProxyURL.URL != nil || len(cfg.ProxyConfig.ProxyConnectHeader.Header) > 0
	}
	if cfg.HTTPHeaders != nil {
		for name := range cfg.HTTPHeaders.Headers {
			state.debugHeaderNames = append(state.debugHeaderNames, name)
		}
	}
	c.stateMut.Lock()
	previousClient := c.state.client
	c.state = state
	c.stateMut.Unlock()
	c.queue.setCapacity(newArgs.Queue.Capacity)
	if previousClient != nil {
		previousClient.CloseIdleConnections()
	}
	return nil
}

func (c *Component) receive(ctx context.Context, alerts []alertpipeline.Alert) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-c.receiveGate:
	}
	defer func() { c.receiveGate <- struct{}{} }()

	state := c.getDeliveryState()
	bodies := make([][]byte, 0, len(alerts))
	for index, alert := range alerts {
		if err := alert.Validate(); err != nil {
			c.metrics.droppedAlerts.WithLabelValues("invalid").Inc()
			return fmt.Errorf("alert %d is invalid: %w", index, err)
		}
		body, err := state.transformer.Transform(alert.Clone())
		if err != nil {
			c.metrics.transformationError.Inc()
			c.metrics.droppedAlerts.WithLabelValues("transformation").Inc()
			return fmt.Errorf("transforming alert %d: %w", index, err)
		}
		if !json.Valid(body) {
			c.metrics.transformationError.Inc()
			c.metrics.droppedAlerts.WithLabelValues("transformation").Inc()
			return fmt.Errorf("transforming alert %d: transformer returned invalid JSON", index)
		}
		bodies = append(bodies, body)
	}
	if err := c.queue.enqueue(ctx, bodies, state.blockOverflow); err != nil {
		reason := "queue_full"
		if errors.Is(err, errStopped) {
			reason = "stopped"
		}
		c.metrics.droppedAlerts.WithLabelValues(reason).Add(float64(len(alerts)))
		return err
	}
	c.metrics.receivedAlerts.Add(float64(len(alerts)))
	c.updateQueueMetric()
	return nil
}

// attemptDelivery sends one request. Retryable failures are scheduled back into
// the queue so ready alerts can continue while this alert is backing off. It
// returns true only when the caller's context interrupted the active request.
func (c *Component) attemptDelivery(ctx context.Context, item queuedPayload) bool {
	state := c.getDeliveryState()
	c.metrics.httpRequests.Inc()
	duration, err := alertpipeline.PostJSON(ctx, state.client, state.endpointURL, item.body, nil, state.timeout, alertpipeline.HTTPDebug{Publisher: c.debugDataPublisher, Retry: item.retries, OmitResponseBody: state.omitDebugResponseBody, ConfiguredHeaders: state.debugHeaderNames})
	c.metrics.httpDuration.Observe(duration.Seconds())
	if err == nil {
		c.queue.complete()
		c.updateQueueMetric()
		c.debugDataPublisher.JSON("[OUT] DELIVERED", 1, item.body)
		c.metrics.sentAlerts.Inc()
		c.setHealth(component.HealthTypeHealthy, "component is ready to accept alerts")
		return false
	}
	if ctx.Err() != nil {
		return true
	}

	failure, ok := err.(*alertpipeline.HTTPFailure)
	if ok {
		c.metrics.httpFailures.WithLabelValues(string(failure.Reason)).Inc()
	}
	if ok && alertpipeline.IsRetryableHTTP(failure, state.retryOnHTTP429) && (state.maxRetries == 0 || item.retries < state.maxRetries) {
		delay := alertpipeline.HTTPRetryDelay(state.minBackoff, state.maxBackoff, item.retries)
		item.retries++
		c.metrics.retries.Inc()
		c.opts.Logger.Warn("retrying transformed alert request", "reason", failure.Reason, "retry", item.retries, "backoff", delay)
		c.queue.retry(item, time.Now().Add(delay))
		c.updateQueueMetric()
		return false
	}

	c.queue.complete()
	c.updateQueueMetric()
	c.metrics.droppedAlerts.WithLabelValues("delivery_failed").Inc()
	c.setHealth(component.HealthTypeUnhealthy, "failed to deliver transformed alert: "+err.Error())
	c.opts.Logger.Error("failed to deliver transformed alert", "err", err)
	return false
}

func (c *Component) drain() error {
	c.queue.stop()
	state := c.getDeliveryState()
	drainCtx, cancel := context.WithTimeout(context.Background(), state.drainTimeout)
	defer cancel()
	for c.queue.len() > 0 {
		item, wait, ready := c.queue.takeReady(time.Now())
		if ready {
			if c.attemptDelivery(drainCtx, item) {
				break
			}
			continue
		}
		if !c.waitForQueue(drainCtx, wait) {
			break
		}
	}
	remaining := c.queue.discard()
	if remaining > 0 {
		c.metrics.droppedAlerts.WithLabelValues("shutdown").Add(float64(remaining))
	}
	c.updateQueueMetric()
	if state.client != nil {
		state.client.CloseIdleConnections()
	}
	c.setHealth(component.HealthTypeUnknown, "component has stopped")
	return nil
}

func (c *Component) waitForQueue(ctx context.Context, wait time.Duration) bool {
	if wait <= 0 {
		select {
		case <-ctx.Done():
			return false
		case <-c.queue.changed:
			return true
		}
	}
	timer := time.NewTimer(wait)
	defer stopTimer(timer)
	select {
	case <-ctx.Done():
		return false
	case <-c.queue.changed:
		return true
	case <-timer.C:
		return true
	}
}

func stopTimer(timer *time.Timer) {
	if !timer.Stop() {
		select {
		case <-timer.C:
		default:
		}
	}
}

func (c *Component) getDeliveryState() deliveryState {
	c.stateMut.RLock()
	defer c.stateMut.RUnlock()
	return c.state
}

func (c *Component) updateQueueMetric() { c.metrics.queueLength.Set(float64(c.queue.len())) }

func (c *Component) setHealth(healthType component.HealthType, message string) {
	c.healthMut.Lock()
	defer c.healthMut.Unlock()
	c.health = component.Health{Health: healthType, Message: message, UpdateTime: time.Now()}
}

// CurrentHealth reports queue and destination delivery health.
func (c *Component) CurrentHealth() component.Health {
	c.healthMut.RLock()
	defer c.healthMut.RUnlock()
	return c.health
}

func (c *Component) LiveDebugging() {}
