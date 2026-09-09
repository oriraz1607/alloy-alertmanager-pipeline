package http_receive

import (
	"fmt"
	"path"
	"strings"
	"time"

	"github.com/alecthomas/units"

	alertpipeline "github.com/grafana/alloy/internal/component/common/alertmanager"
	fnet "github.com/grafana/alloy/internal/component/common/net"
)

// Arguments configures prometheus.alertmanager.http_receive.
type Arguments struct {
	Server             *fnet.ServerConfig       `alloy:",squash"`
	Path               string                   `alloy:"path,attr,optional"`
	Decoder            alertpipeline.Decoder    `alloy:"decoder,attr"`
	MaxRequestBodySize units.Base2Bytes         `alloy:"max_request_body_size,attr,optional"`
	ForwardTimeout     time.Duration            `alloy:"forward_timeout,attr,optional"`
	ForwardTo          []alertpipeline.Receiver `alloy:"forward_to,attr"`
}

// SetToDefault implements syntax.Defaulter.
func (args *Arguments) SetToDefault() {
	server := fnet.DefaultServerConfig()
	server.HTTP.ListenAddress = "127.0.0.1"
	server.HTTP.ListenPort = 5002
	*args = Arguments{
		Server:             server,
		Path:               "/alerts",
		MaxRequestBodySize: units.MiB,
		ForwardTimeout:     10 * time.Second,
	}
}

// Validate implements syntax.Validator.
func (args *Arguments) Validate() error {
	if args.Server == nil || args.Server.HTTP == nil {
		return fmt.Errorf("HTTP server configuration is missing")
	}
	if args.Path == "" || !strings.HasPrefix(args.Path, "/") {
		return fmt.Errorf("path must start with /")
	}
	if strings.ContainsAny(args.Path, "?#") {
		return fmt.Errorf("path must not contain a query string or fragment")
	}
	if path.Clean(args.Path) != args.Path {
		return fmt.Errorf("path must be a clean URL path")
	}
	if args.Decoder == nil {
		return fmt.Errorf("decoder must be configured")
	}
	if len(args.ForwardTo) == 0 {
		return fmt.Errorf("forward_to must contain at least one receiver")
	}
	if args.MaxRequestBodySize <= 0 {
		return fmt.Errorf("max_request_body_size must be greater than 0")
	}
	if args.ForwardTimeout <= 0 {
		return fmt.Errorf("forward_timeout must be greater than 0")
	}
	return nil
}
