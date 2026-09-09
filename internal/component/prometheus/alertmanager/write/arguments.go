package write

import (
	"fmt"
	"net/url"
	"time"

	commonconfig "github.com/grafana/alloy/internal/component/common/config"
)

const defaultAlertsPath = "/api/v2/alerts"

// Arguments configures prometheus.alertmanager.write.
type Arguments struct {
	Endpoint EndpointArguments `alloy:"endpoint,block"`
	Queue    QueueArguments    `alloy:"queue_config,block,optional"`

	RefreshInterval     time.Duration `alloy:"refresh_interval,attr,optional"`
	FiringAlertDuration time.Duration `alloy:"firing_alert_duration,attr,optional"`
	FiringAlertTimeout  time.Duration `alloy:"firing_alert_timeout,attr,optional"`
	ResolvedRetention   time.Duration `alloy:"resolved_retention,attr,optional"`
}

// SetToDefault implements syntax.Defaulter.
func (args *Arguments) SetToDefault() {
	*args = Arguments{
		RefreshInterval:     time.Minute,
		FiringAlertDuration: 5 * time.Minute,
		FiringAlertTimeout:  5 * time.Hour,
		ResolvedRetention:   5 * time.Minute,
	}
	args.Endpoint.SetToDefault()
	args.Queue.SetToDefault()
}

// Validate implements syntax.Validator.
func (args *Arguments) Validate() error {
	if err := args.Endpoint.Validate(); err != nil {
		return err
	}
	if err := args.Queue.Validate(args.Endpoint.BatchSize); err != nil {
		return err
	}
	if args.RefreshInterval <= 0 {
		return fmt.Errorf("refresh_interval must be greater than 0")
	}
	if args.FiringAlertDuration <= args.RefreshInterval {
		return fmt.Errorf("firing_alert_duration must be greater than refresh_interval")
	}
	if args.FiringAlertTimeout <= args.RefreshInterval {
		return fmt.Errorf("firing_alert_timeout must be greater than refresh_interval")
	}
	if args.ResolvedRetention < 0 {
		return fmt.Errorf("resolved_retention must not be negative")
	}
	return nil
}

// EndpointArguments configures the destination Alertmanager and delivery policy.
type EndpointArguments struct {
	URL              string                         `alloy:"url,attr"`
	Timeout          time.Duration                  `alloy:"timeout,attr,optional"`
	BatchSize        int                            `alloy:"batch_size,attr,optional"`
	BatchWait        time.Duration                  `alloy:"batch_wait,attr,optional"`
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
		BatchSize:        1,
		BatchWait:        time.Second,
		MinBackoff:       500 * time.Millisecond,
		MaxBackoff:       5 * time.Minute,
		MaxRetries:       10,
		RetryOnHTTP429:   true,
		HTTPClientConfig: commonconfig.CloneDefaultHTTPClientConfig(),
	}
}

// Validate implements syntax.Validator.
func (args *EndpointArguments) Validate() error {
	if _, err := normalizeEndpointURL(args.URL); err != nil {
		return err
	}
	if args.Timeout <= 0 {
		return fmt.Errorf("endpoint timeout must be greater than 0")
	}
	if args.BatchSize <= 0 {
		return fmt.Errorf("endpoint batch_size must be greater than 0")
	}
	if args.BatchWait < 0 {
		return fmt.Errorf("endpoint batch_wait must not be negative")
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

// QueueArguments controls the bounded alert queue.
type QueueArguments struct {
	Persistent      bool          `alloy:"persistent,attr,optional"`
	Directory       string        `alloy:"directory,attr,optional"`
	MaxDiskBytes    int64         `alloy:"max_disk_bytes,attr,optional"`
	Capacity        int           `alloy:"capacity,attr,optional"`
	DrainTimeout    time.Duration `alloy:"drain_timeout,attr,optional"`
	BlockOnOverflow bool          `alloy:"block_on_overflow,attr,optional"`
}

// SetToDefault implements syntax.Defaulter.
func (args *QueueArguments) SetToDefault() {
	*args = QueueArguments{
		Capacity:        1000,
		MaxDiskBytes:    64 * 1024 * 1024,
		DrainTimeout:    15 * time.Second,
		BlockOnOverflow: true,
	}
}

// Validate checks queue settings against the configured maximum batch size.
func (args QueueArguments) Validate(batchSize int) error {
	if args.MaxDiskBytes < 4096 {
		return fmt.Errorf("queue_config max_disk_bytes must be at least 4096")
	}
	if args.Directory != "" && !args.Persistent {
		return fmt.Errorf("queue_config directory requires persistent = true")
	}
	if args.Capacity <= 0 {
		return fmt.Errorf("queue_config capacity must be greater than 0")
	}
	if args.Capacity < batchSize {
		return fmt.Errorf("queue_config capacity must be greater than or equal to endpoint batch_size")
	}
	if args.DrainTimeout <= 0 {
		return fmt.Errorf("queue_config drain_timeout must be greater than 0")
	}
	return nil
}

func normalizeEndpointURL(rawURL string) (string, error) {
	u, err := url.Parse(rawURL)
	if err != nil {
		return "", fmt.Errorf("invalid endpoint URL: %w", err)
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return "", fmt.Errorf("endpoint URL scheme must be http or https")
	}
	if u.Host == "" {
		return "", fmt.Errorf("endpoint URL must include a host")
	}
	if u.User != nil {
		return "", fmt.Errorf("endpoint URL must not contain user information; configure an authentication block instead")
	}
	if u.Fragment != "" {
		return "", fmt.Errorf("endpoint URL must not contain a fragment")
	}
	if u.Path == "" {
		u.Path = defaultAlertsPath
	}
	return u.String(), nil
}
