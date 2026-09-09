package http_receive

import (
	"context"
	"fmt"
	"net/http"
	"reflect"
	"sync"
	"time"

	"github.com/gorilla/mux"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/grafana/alloy/internal/component"
	alertpipeline "github.com/grafana/alloy/internal/component/common/alertmanager"
	fnet "github.com/grafana/alloy/internal/component/common/net"
	"github.com/grafana/alloy/internal/util"
)

func init() {
	component.Register(component.Registration{
		Name:      "prometheus.alertmanager.http_receive",
		Community: true,
		Args:      Arguments{},
		Build: func(opts component.Options, args component.Arguments) (component.Component, error) {
			return New(opts, args.(Arguments))
		},
	})
}

type handlerState struct {
	path               string
	decoder            alertpipeline.Decoder
	maxRequestBodySize int64
	forwardTimeout     time.Duration
}

// Component receives configured JSON on a configured path and emits typed alerts.
type Component struct {
	debugDataPublisher alertpipeline.DebugPublisher
	opts               component.Options
	metrics            *metrics
	fanout             *alertpipeline.Fanout

	stateMut sync.RWMutex
	state    handlerState

	updateMut          sync.Mutex
	server             *fnet.TargetServer
	serverConfig       *fnet.ServerConfig
	listenPath         string
	uncheckedCollector *util.UncheckedCollector

	healthMut sync.RWMutex
	health    component.Health
}

var (
	_ component.LiveDebugging   = (*Component)(nil)
	_ component.Component       = (*Component)(nil)
	_ component.HealthComponent = (*Component)(nil)
)

// New creates a prometheus.alertmanager.http_receive component.
func New(opts component.Options, args Arguments) (*Component, error) {
	uncheckedCollector := util.NewUncheckedCollector(nil)
	opts.Registerer.MustRegister(uncheckedCollector)
	c := &Component{
		debugDataPublisher: alertpipeline.NewDebugPublisher(opts),
		opts:               opts,
		metrics:            newMetrics(opts.Registerer),
		fanout:             alertpipeline.NewFanout(nil),
		uncheckedCollector: uncheckedCollector,
		health:             component.Health{Health: component.HealthTypeUnknown, Message: "component is starting", UpdateTime: time.Now()},
	}
	if err := c.Update(args); err != nil {
		return nil, err
	}
	return c, nil
}

// Run waits for shutdown and gracefully stops the custom JSON server.
func (c *Component) Run(ctx context.Context) error {
	<-ctx.Done()
	c.updateMut.Lock()
	c.shutdownServer()
	c.updateMut.Unlock()
	c.fanout.Clear()
	c.setHealth(component.HealthTypeUnknown, "component has stopped")
	return nil
}

// Update applies decoder, route, listener, and downstream configuration.
func (c *Component) Update(args component.Arguments) error {
	newArgs := args.(Arguments)
	if err := newArgs.Validate(); err != nil {
		return err
	}

	c.updateMut.Lock()
	defer c.updateMut.Unlock()
	restart := c.server == nil || !reflect.DeepEqual(c.serverConfig, newArgs.Server) || c.listenPath != newArgs.Path
	if restart {
		c.shutdownServer()
	}
	c.fanout.UpdateChildren(newArgs.ForwardTo)
	c.stateMut.Lock()
	c.state = handlerState{
		path:               newArgs.Path,
		decoder:            newArgs.Decoder,
		maxRequestBodySize: int64(newArgs.MaxRequestBodySize),
		forwardTimeout:     newArgs.ForwardTimeout,
	}
	c.stateMut.Unlock()
	if !restart {
		return nil
	}

	serverRegistry := prometheus.NewRegistry()
	c.uncheckedCollector.SetCollector(serverRegistry)
	server, err := fnet.NewTargetServer(c.opts.Logger, "prometheus_alertmanager_http_receive", serverRegistry, newArgs.Server)
	if err != nil {
		return fmt.Errorf("creating custom alert HTTP server: %w", err)
	}
	if err := server.MountAndRun(func(router *mux.Router) {
		router.Path(newArgs.Path).Handler(http.HandlerFunc(c.handleRequest))
	}); err != nil {
		return fmt.Errorf("starting custom alert HTTP server: %w", err)
	}
	c.server = server
	c.serverConfig = newArgs.Server
	c.listenPath = newArgs.Path
	c.setHealth(component.HealthTypeHealthy, "component is ready to receive custom alert JSON")
	c.opts.Logger.Info("started custom Alertmanager JSON receiver", "addr", server.HTTPListenAddr(), "path", newArgs.Path)
	return nil
}

func (c *Component) getHandlerState() handlerState {
	c.stateMut.RLock()
	defer c.stateMut.RUnlock()
	return c.state
}

func (c *Component) shutdownServer() {
	if c.server == nil {
		return
	}
	c.server.StopAndShutdown()
	c.server = nil
	c.serverConfig = nil
	c.listenPath = ""
}

func (c *Component) setHealth(healthType component.HealthType, message string) {
	c.healthMut.Lock()
	defer c.healthMut.Unlock()
	c.health = component.Health{Health: healthType, Message: message, UpdateTime: time.Now()}
}

// CurrentHealth reports whether the configured HTTP listener is running.
func (c *Component) CurrentHealth() component.Health {
	c.healthMut.RLock()
	defer c.healthMut.RUnlock()
	return c.health
}

func (c *Component) LiveDebugging() {}
