package http_receive

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/grafana/alloy/internal/util"
)

type metrics struct {
	requests           prometheus.Counter
	activeRequests     prometheus.Gauge
	requestDuration    prometheus.Histogram
	invalidRequests    *prometheus.CounterVec
	receivedAlerts     prometheus.Counter
	forwardedAlerts    prometheus.Counter
	forwardingFailures prometheus.Counter
}

func newMetrics(reg prometheus.Registerer) *metrics {
	m := &metrics{
		requests:           prometheus.NewCounter(prometheus.CounterOpts{Name: "prometheus_alertmanager_http_receive_decode_requests_total", Help: "Total requests handled by the custom alert decoder."}),
		activeRequests:     prometheus.NewGauge(prometheus.GaugeOpts{Name: "prometheus_alertmanager_http_receive_active_decode_requests", Help: "Current custom alert decode requests being processed."}),
		requestDuration:    prometheus.NewHistogram(prometheus.HistogramOpts{Name: "prometheus_alertmanager_http_receive_decode_request_duration_seconds", Help: "Duration of custom alert decode requests.", Buckets: prometheus.DefBuckets, NativeHistogramBucketFactor: 1.1, NativeHistogramMaxBucketNumber: 100, NativeHistogramMinResetDuration: time.Hour}),
		invalidRequests:    prometheus.NewCounterVec(prometheus.CounterOpts{Name: "prometheus_alertmanager_http_receive_invalid_requests_total", Help: "Rejected custom alert requests by reason."}, []string{"reason"}),
		receivedAlerts:     prometheus.NewCounter(prometheus.CounterOpts{Name: "prometheus_alertmanager_http_receive_received_alerts_total", Help: "Custom JSON bodies decoded into valid typed alerts."}),
		forwardedAlerts:    prometheus.NewCounter(prometheus.CounterOpts{Name: "prometheus_alertmanager_http_receive_forwarded_alerts_total", Help: "Decoded alerts accepted by every downstream receiver."}),
		forwardingFailures: prometheus.NewCounter(prometheus.CounterOpts{Name: "prometheus_alertmanager_http_receive_forwarding_failures_total", Help: "Decoded alerts rejected by a downstream receiver."}),
	}
	m.requests = util.MustRegisterOrGet(reg, m.requests).(prometheus.Counter)
	m.activeRequests = util.MustRegisterOrGet(reg, m.activeRequests).(prometheus.Gauge)
	m.requestDuration = util.MustRegisterOrGet(reg, m.requestDuration).(prometheus.Histogram)
	m.invalidRequests = util.MustRegisterOrGet(reg, m.invalidRequests).(*prometheus.CounterVec)
	m.receivedAlerts = util.MustRegisterOrGet(reg, m.receivedAlerts).(prometheus.Counter)
	m.forwardedAlerts = util.MustRegisterOrGet(reg, m.forwardedAlerts).(prometheus.Counter)
	m.forwardingFailures = util.MustRegisterOrGet(reg, m.forwardingFailures).(prometheus.Counter)
	return m
}
