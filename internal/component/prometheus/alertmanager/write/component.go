package write

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"path/filepath"
	"sync"
	"time"

	promconfig "github.com/prometheus/common/config"

	"github.com/grafana/alloy/internal/component"
	alertpipeline "github.com/grafana/alloy/internal/component/common/alertmanager"
	"github.com/grafana/alloy/internal/useragent"
)

func init() {
	component.Register(component.Registration{
		Name:      "prometheus.alertmanager.write",
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
	client              *http.Client
	endpointURL         string
	timeout             time.Duration
	batchSize           int
	batchWait           time.Duration
	minBackoff          time.Duration
	maxBackoff          time.Duration
	maxRetries          int
	retryOnHTTP429      bool
	drainTimeout        time.Duration
	blockOnOverflow     bool
	refreshInterval     time.Duration
	firingAlertDuration time.Duration
	firingAlertTimeout  time.Duration
	resolvedRetention   time.Duration
}

type receiver struct {
	component *Component
	id        string
}

func (r *receiver) Send(ctx context.Context, alerts []alertpipeline.Alert) error {
	return r.component.receive(ctx, alerts)
}

func (r *receiver) String() string { return r.id + ".receiver" }

// Component accepts typed alerts and writes them to the Alertmanager API.
type Component struct {
	debugDataPublisher alertpipeline.DebugPublisher
	opts               component.Options
	metrics            *metrics
	queue              *alertQueue
	tracker            *alertTracker
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

// New creates a prometheus.alertmanager.write component.
func New(opts component.Options, args Arguments) (*Component, error) {
	if err := args.Validate(); err != nil {
		return nil, err
	}
	c := &Component{
		debugDataPublisher: alertpipeline.NewDebugPublisher(opts),
		opts:               opts,
		metrics:            newMetrics(opts.Registerer),
		queue:              newAlertQueue(args.Queue.Capacity),
		tracker:            newAlertTracker(),
		receiveGate:        make(chan struct{}, 1),
		health: component.Health{
			Health:     component.HealthTypeUnknown,
			Message:    "component is starting",
			UpdateTime: time.Now(),
		},
	}
	if args.Queue.Persistent {
		directory := args.Queue.Directory
		if directory == "" && opts.DataPath != "" {
			directory = filepath.Join(opts.DataPath, "queue")
		}
		store, pending, err := openQueueStore(directory, args, c.tracker)
		if err != nil {
			return nil, fmt.Errorf("opening persistent alert queue: %w", err)
		}
		c.queue.store, c.queue.items = store, pending
	}
	c.receiveGate <- struct{}{}
	c.receiver = &receiver{component: c, id: opts.ID}
	if err := c.Update(args); err != nil {
		if c.queue.store != nil {
			_ = c.queue.store.lock.Unlock()
		}
		return nil, err
	}
	c.updateQueueMetric()
	c.updateTrackerMetrics()
	opts.OnStateChange(Exports{Receiver: c.receiver})
	return c, nil
}

// Run sends queued alerts, refreshes tracked state, and drains during shutdown.
func (c *Component) Run(ctx context.Context) error {
	if c.queue.store != nil {
		defer func() { _ = c.queue.store.lock.Unlock() }()
	}
	defer c.queue.stop()
	c.setHealth(component.HealthTypeHealthy, "component is ready to accept alerts")
	lastRefresh := time.Now()

	for {
		nextRefresh := lastRefresh.Add(c.getDeliveryState().refreshInterval)
		if c.queue.len() > 0 {
			batch, ok := c.collectBatch(ctx)
			if !ok {
				return c.drain()
			}
			if err := c.deliver(ctx, batch, c.queue.store != nil); err != nil && ctx.Err() != nil {
				c.queue.prepend(batch)
				c.updateQueueMetric()
				return c.drain()
			}
			if err := c.queue.acknowledge(); err != nil {
				return fmt.Errorf("acknowledging alert delivery: %w", err)
			}
			c.updateQueueMetric()
		}

		if c.queue.len() == 0 && !time.Now().Before(nextRefresh) {
			if err := c.refresh(ctx); err != nil && ctx.Err() != nil {
				return c.drain()
			}
			lastRefresh = time.Now()
			continue
		}

		if c.queue.len() > 0 {
			continue
		}
		timer := time.NewTimer(time.Until(nextRefresh))
		select {
		case <-ctx.Done():
			stopTimer(timer)
			return c.drain()
		case <-c.queue.changed:
			stopTimer(timer)
		case <-timer.C:
		}
	}
}

// Update applies endpoint, batching, queue, retry, and refresh settings.
func (c *Component) Update(args component.Arguments) error {
	newArgs := args.(Arguments)
	if err := newArgs.Validate(); err != nil {
		return err
	}
	if newArgs.Queue.Persistent != (c.queue.store != nil) {
		return fmt.Errorf("changing persistent requires restarting the component")
	}
	if store := c.queue.store; store != nil {
		directory := newArgs.Queue.Directory
		if directory == "" && c.opts.DataPath != "" {
			directory = filepath.Join(c.opts.DataPath, "queue")
		}
		if directory != store.directory || newArgs.Queue.MaxDiskBytes/2 != store.maxBytes {
			return fmt.Errorf("changing persistence directory or disk limit requires restarting the component")
		}
	}
	endpointURL, err := normalizeEndpointURL(newArgs.Endpoint.URL)
	if err != nil {
		return err
	}
	client, err := promconfig.NewClientFromConfig(
		*newArgs.Endpoint.HTTPClientConfig.Convert(),
		c.opts.ID,
		promconfig.WithUserAgent(useragent.Get()),
	)
	if err != nil {
		return fmt.Errorf("creating destination HTTP client: %w", err)
	}

	state := deliveryState{
		client:              client,
		endpointURL:         endpointURL,
		timeout:             newArgs.Endpoint.Timeout,
		batchSize:           newArgs.Endpoint.BatchSize,
		batchWait:           newArgs.Endpoint.BatchWait,
		minBackoff:          newArgs.Endpoint.MinBackoff,
		maxBackoff:          newArgs.Endpoint.MaxBackoff,
		maxRetries:          newArgs.Endpoint.MaxRetries,
		retryOnHTTP429:      newArgs.Endpoint.RetryOnHTTP429,
		drainTimeout:        newArgs.Queue.DrainTimeout,
		blockOnOverflow:     newArgs.Queue.BlockOnOverflow,
		refreshInterval:     newArgs.RefreshInterval,
		firingAlertDuration: newArgs.FiringAlertDuration,
		firingAlertTimeout:  newArgs.FiringAlertTimeout,
		resolvedRetention:   newArgs.ResolvedRetention,
	}
	c.stateMut.Lock()
	previousClient := c.state.client
	c.state = state
	c.stateMut.Unlock()
	c.queue.mut.Lock()
	if c.queue.store != nil {
		c.queue.store.retention = newArgs.ResolvedRetention
		c.queue.store.firingTimeout = newArgs.FiringAlertTimeout
	}
	c.queue.mut.Unlock()
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

	for i, alert := range alerts {
		if err := alert.Validate(); err != nil {
			c.metrics.droppedAlerts.WithLabelValues("invalid").Inc()
			return fmt.Errorf("alert %d is invalid: %w", i, err)
		}
	}
	state := c.getDeliveryState()
	if err := c.queue.enqueue(ctx, alerts, state.blockOnOverflow, func() {
		c.tracker.observe(alerts, time.Now(), state.resolvedRetention)
	}); err != nil {
		reason := "queue_full"
		if c.queue.store != nil && !errors.Is(err, errQueueFull) && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
			reason = "persistence"
		}
		if errors.Is(err, errStopped) {
			reason = "stopped"
		}
		c.metrics.droppedAlerts.WithLabelValues(reason).Add(float64(len(alerts)))
		c.setHealth(component.HealthTypeUnhealthy, err.Error())
		return err
	}
	for _, alert := range alerts {
		c.debugDataPublisher.Alert("[IN]", 1, alert)
	}
	c.metrics.receivedAlerts.Add(float64(len(alerts)))
	c.updateQueueMetric()
	c.updateTrackerMetrics()
	return nil
}

func stopTimer(timer *time.Timer) {
	if !timer.Stop() {
		select {
		case <-timer.C:
		default:
		}
	}
}

func (c *Component) collectBatch(ctx context.Context) ([]alertpipeline.Alert, bool) {
	state := c.getDeliveryState()
	if c.queue.len() >= state.batchSize || state.batchWait == 0 {
		batch := c.queue.dequeue(state.batchSize)
		c.updateQueueMetric()
		return batch, true
	}

	timer := time.NewTimer(state.batchWait)
	defer timer.Stop()
	for c.queue.len() < state.batchSize {
		select {
		case <-ctx.Done():
			return nil, false
		case <-timer.C:
			batch := c.queue.dequeue(state.batchSize)
			c.updateQueueMetric()
			return batch, true
		case <-c.queue.changed:
		}
	}
	batch := c.queue.dequeue(state.batchSize)
	c.updateQueueMetric()
	return batch, true
}

func (c *Component) deliver(ctx context.Context, alerts []alertpipeline.Alert, preserve bool) error {
	if len(alerts) == 0 {
		return nil
	}
	err := c.sendWithRetry(ctx, alerts, preserve)
	if err != nil {
		if ctx.Err() == nil {
			c.metrics.droppedAlerts.WithLabelValues("delivery_failed").Add(float64(len(alerts)))
			c.setHealth(component.HealthTypeUnhealthy, "failed to deliver alerts: "+err.Error())
			c.opts.Logger.Error("failed to deliver alerts to Alertmanager", "alert_count", len(alerts), "err", err)
		}
		return err
	}
	c.metrics.sentAlerts.Add(float64(len(alerts)))
	c.setHealth(component.HealthTypeHealthy, "component is ready to accept alerts")
	return nil
}

func (c *Component) refresh(ctx context.Context) error {
	state := c.getDeliveryState()
	alerts, expired := c.tracker.snapshot(time.Now(), state.firingAlertTimeout)
	if expired > 0 {
		c.metrics.expiredFiringAlerts.Add(float64(expired))
		c.opts.Logger.Warn("stopped refreshing stale firing alerts", "alert_count", expired)
	}
	c.updateTrackerMetrics()
	for len(alerts) > 0 {
		count := min(state.batchSize, len(alerts))
		if err := c.deliver(ctx, alerts[:count], false); err != nil && ctx.Err() != nil {
			return err
		}
		alerts = alerts[count:]
	}
	return nil
}

func (c *Component) drain() error {
	c.queue.stop()
	state := c.getDeliveryState()
	drainCtx, cancel := context.WithTimeout(context.Background(), state.drainTimeout)
	defer cancel()

	for c.queue.len() > 0 {
		batch := c.queue.dequeue(state.batchSize)
		c.updateQueueMetric()
		if err := c.deliver(drainCtx, batch, c.queue.store != nil); err != nil && drainCtx.Err() != nil {
			if c.queue.store != nil {
				c.queue.prepend(batch)
			} else {
				c.metrics.droppedAlerts.WithLabelValues("shutdown").Add(float64(len(batch)))
			}
			break
		}
		if err := c.queue.acknowledge(); err != nil {
			return err
		}
	}
	if c.queue.store != nil {
		c.updateQueueMetric()
		state.client.CloseIdleConnections()
		c.setHealth(component.HealthTypeUnknown, "component stopped with pending alerts preserved")
		return nil
	}
	remaining := c.queue.dequeue(c.queue.len())
	if len(remaining) > 0 {
		c.metrics.droppedAlerts.WithLabelValues("shutdown").Add(float64(len(remaining)))
	}
	c.updateQueueMetric()
	if state.client != nil {
		state.client.CloseIdleConnections()
	}
	c.setHealth(component.HealthTypeUnknown, "component has stopped")
	return nil
}

func (c *Component) getDeliveryState() deliveryState {
	c.stateMut.RLock()
	defer c.stateMut.RUnlock()
	return c.state
}

func (c *Component) updateQueueMetric() {
	c.queue.mut.Lock()
	defer c.queue.mut.Unlock()
	c.metrics.queueLength.Set(float64(len(c.queue.items) + len(c.queue.inflight)))
}

func (c *Component) updateTrackerMetrics() {
	firing, resolved := c.tracker.counts()
	c.metrics.trackedFiringAlerts.Set(float64(firing))
	c.metrics.trackedResolvedAlerts.Set(float64(resolved))
}

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
