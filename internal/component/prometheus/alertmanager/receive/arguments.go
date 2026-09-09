package receive

import (
	"fmt"
	"path"
	"strings"
	"time"

	"github.com/alecthomas/units"

	alertpipeline "github.com/grafana/alloy/internal/component/common/alertmanager"
	fnet "github.com/grafana/alloy/internal/component/common/net"
)

// Arguments configures prometheus.alertmanager.receive.
type Arguments struct {
	Server             *fnet.ServerConfig       `alloy:",squash"`
	WebhookPath        string                   `alloy:"webhook_path,attr,optional"`
	MaxRequestBodySize units.Base2Bytes         `alloy:"max_request_body_size,attr,optional"`
	ForwardTimeout     time.Duration            `alloy:"forward_timeout,attr,optional"`
	ForwardTo          []alertpipeline.Receiver `alloy:"forward_to,attr"`
}

// SetToDefault implements syntax.Defaulter.
func (args *Arguments) SetToDefault() {
	server := fnet.DefaultServerConfig()
	server.HTTP.ListenAddress = "127.0.0.1"
	server.HTTP.ListenPort = 5001
	*args = Arguments{
		Server:             server,
		WebhookPath:        "/webhook",
		MaxRequestBodySize: units.MiB,
		ForwardTimeout:     10 * time.Second,
	}
}

// Validate implements syntax.Validator.
func (args *Arguments) Validate() error {
	if args.Server == nil || args.Server.HTTP == nil {
		return fmt.Errorf("HTTP server configuration is missing")
	}
	if args.WebhookPath == "" || !strings.HasPrefix(args.WebhookPath, "/") {
		return fmt.Errorf("webhook_path must start with /")
	}
	if path.Clean(args.WebhookPath) != args.WebhookPath {
		return fmt.Errorf("webhook_path must be a clean URL path")
	}
	if args.MaxRequestBodySize <= 0 {
		return fmt.Errorf("max_request_body_size must be greater than 0")
	}
	if args.ForwardTimeout <= 0 {
		return fmt.Errorf("forward_timeout must be greater than 0")
	}
	return nil
}
