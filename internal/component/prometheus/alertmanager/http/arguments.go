package http

import (
	"fmt"
	"net/url"
	"time"

	alertpipeline "github.com/grafana/alloy/internal/component/common/alertmanager"
	commonconfig "github.com/grafana/alloy/internal/component/common/config"
)

// Arguments configures prometheus.alertmanager.http.
type Arguments struct {
	Transformer alertpipeline.Transformer `alloy:"transformer,attr"`
	Endpoint    EndpointArguments         `alloy:"endpoint,block"`
	Queue       QueueArguments            `alloy:"queue_config,block,optional"`
}

// SetToDefault implements syntax.Defaulter.
func (args *Arguments) SetToDefault() {
	*args = Arguments{}
	args.Endpoint.SetToDefault()
	args.Queue.SetToDefault()
}

// Validate implements syntax.Validator.
func (args *Arguments) Validate() error {
	if args.Transformer == nil {
		return fmt.Errorf("transformer must be configured")
	}
	if err := args.Endpoint.Validate(); err != nil {
		return err
	}
	return args.Queue.Validate()
}

// EndpointArguments configures the arbitrary JSON destination.
type EndpointArguments struct {
	URL              string                         `alloy:"url,attr"`
	Timeout          time.Duration                  `alloy:"timeout,attr,optional"`
	MinBackoff       time.Duration                  `alloy:"min_backoff_period,attr,optional"`
	MaxBackoff       time.Duration                  `alloy:"max_backoff_period,attr,optional"`
	MaxRetries       int                            `alloy:"max_retries,attr,optional"`
	RetryOnHTTP429   bool                           `alloy:"retry_on_http_429,attr,optional"`
	HTTPClientConfig *commonconfig.HTTPClientConfig `alloy:",squash"`
}

// SetToDefault implements syntax.Defaulter.
func (args *EndpointArguments) SetToDefault() {
	*args = EndpointArguments{
		Timeout:          10 * time.Second,
		MinBackoff:       500 * time.Millisecond,
		MaxBackoff:       5 * time.Minute,
		MaxRetries:       10,
		RetryOnHTTP429:   true,
		HTTPClientConfig: commonconfig.CloneDefaultHTTPClientConfig(),
	}
}

// Validate verifies the endpoint and retry policy.
func (args *EndpointArguments) Validate() error {
	parsed, err := url.Parse(args.URL)
	if err != nil {
		return fmt.Errorf("invalid endpoint URL: %w", err)
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return fmt.Errorf("endpoint URL scheme must be http or https")
	}
	if parsed.Host == "" {
		return fmt.Errorf("endpoint URL must include a host")
	}
	if parsed.User != nil {
		return fmt.Errorf("endpoint URL must not contain user information; configure an authentication block instead")
	}
	if parsed.Fragment != "" {
		return fmt.Errorf("endpoint URL must not contain a fragment")
	}
	if args.Timeout <= 0 {
		return fmt.Errorf("endpoint timeout must be greater than 0")
	}
	if args.MinBackoff <= 0 {
		return fmt.Errorf("endpoint min_backoff_period must be greater than 0")
	}
	if args.MaxBackoff < args.MinBackoff {
		return fmt.Errorf("endpoint max_backoff_period must be greater than or equal to min_backoff_period")
	}
	if args.MaxRetries < 0 {
		return fmt.Errorf("endpoint max_retries must not be negative")
	}
	if args.HTTPClientConfig == nil {
		return fmt.Errorf("endpoint HTTP client configuration is missing")
	}
	if err := args.HTTPClientConfig.Validate(); err != nil {
		return fmt.Errorf("invalid endpoint HTTP client configuration: %w", err)
	}
	return nil
}

// QueueArguments configures the bounded transformed-payload queue.
type QueueArguments struct {
	Capacity        int           `alloy:"capacity,attr,optional"`
	DrainTimeout    time.Duration `alloy:"drain_timeout,attr,optional"`
	BlockOnOverflow bool          `alloy:"block_on_overflow,attr,optional"`
}

// SetToDefault implements syntax.Defaulter.
func (args *QueueArguments) SetToDefault() {
	*args = QueueArguments{Capacity: 1000, DrainTimeout: 15 * time.Second, BlockOnOverflow: true}
}

// Validate verifies queue settings.
func (args QueueArguments) Validate() error {
	if args.Capacity <= 0 {
		return fmt.Errorf("queue_config capacity must be greater than 0")
	}
	if args.DrainTimeout <= 0 {
		return fmt.Errorf("queue_config drain_timeout must be greater than 0")
	}
	return nil
}
