package receive

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/grafana/alloy/internal/util"
)

type metrics struct {
	webhookRequests       prometheus.Counter
	invalidRequests       prometheus.Counter
	requestDuration       prometheus.Histogram
	receivedAlerts        prometheus.Counter
	forwardedAlerts       prometheus.Counter
	forwardingFailures    prometheus.Counter
	activeWebhookRequests prometheus.Gauge
}

func newMetrics(reg prometheus.Registerer) *metrics {
	m := &metrics{
		webhookRequests: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "prometheus_alertmanager_receive_webhook_requests_total",
			Help: "Total number of Alertmanager webhook requests received.",
		}),
		invalidRequests: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "prometheus_alertmanager_receive_invalid_requests_total",
			Help: "Total number of Alertmanager webhook requests rejected as invalid.",
		}),
		requestDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:                            "prometheus_alertmanager_receive_webhook_request_duration_seconds",
			Help:                            "Duration of Alertmanager webhook requests.",
			Buckets:                         prometheus.DefBuckets,
			NativeHistogramBucketFactor:     1.1,
			NativeHistogramMaxBucketNumber:  100,
			NativeHistogramMinResetDuration: time.Hour,
		}),
		receivedAlerts: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "prometheus_alertmanager_receive_received_alerts_total",
			Help: "Total number of alerts received in valid webhook envelopes.",
		}),
		forwardedAlerts: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "prometheus_alertmanager_receive_forwarded_alerts_total",
			Help: "Total number of alerts accepted by every configured downstream receiver.",
		}),
		forwardingFailures: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "prometheus_alertmanager_receive_forwarding_failures_total",
			Help: "Total number of webhook deliveries rejected by a downstream receiver.",
		}),
		activeWebhookRequests: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "prometheus_alertmanager_receive_active_webhook_requests",
			Help: "Current number of active Alertmanager webhook requests.",
		}),
	}

	m.webhookRequests = util.MustRegisterOrGet(reg, m.webhookRequests).(prometheus.Counter)
	m.invalidRequests = util.MustRegisterOrGet(reg, m.invalidRequests).(prometheus.Counter)
	m.requestDuration = util.MustRegisterOrGet(reg, m.requestDuration).(prometheus.Histogram)
	m.receivedAlerts = util.MustRegisterOrGet(reg, m.receivedAlerts).(prometheus.Counter)
	m.forwardedAlerts = util.MustRegisterOrGet(reg, m.forwardedAlerts).(prometheus.Counter)
	m.forwardingFailures = util.MustRegisterOrGet(reg, m.forwardingFailures).(prometheus.Counter)
	m.activeWebhookRequests = util.MustRegisterOrGet(reg, m.activeWebhookRequests).(prometheus.Gauge)
	return m
}
