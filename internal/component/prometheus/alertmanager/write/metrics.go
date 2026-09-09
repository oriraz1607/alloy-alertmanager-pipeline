package write

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/grafana/alloy/internal/util"
)

type metrics struct {
	receivedAlerts        prometheus.Counter
	sentAlerts            prometheus.Counter
	httpRequests          prometheus.Counter
	httpRequestFailures   *prometheus.CounterVec
	httpRequestDuration   prometheus.Histogram
	retries               prometheus.Counter
	droppedAlerts         *prometheus.CounterVec
	expiredFiringAlerts   prometheus.Counter
	queueLength           prometheus.Gauge
	trackedFiringAlerts   prometheus.Gauge
	trackedResolvedAlerts prometheus.Gauge
}

func newMetrics(reg prometheus.Registerer) *metrics {
	m := &metrics{
		receivedAlerts: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "prometheus_alertmanager_write_received_alerts_total",
			Help: "Total number of alerts received from upstream Alloy components.",
		}),
		sentAlerts: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "prometheus_alertmanager_write_sent_alerts_total",
			Help: "Total number of alerts accepted by the destination Alertmanager.",
		}),
		httpRequests: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "prometheus_alertmanager_write_http_requests_total",
			Help: "Total number of HTTP requests sent to the destination Alertmanager.",
		}),
		httpRequestFailures: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "prometheus_alertmanager_write_http_request_failures_total",
			Help: "Total number of failed requests to the destination Alertmanager.",
		}, []string{"reason"}),
		httpRequestDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:                            "prometheus_alertmanager_write_http_request_duration_seconds",
			Help:                            "Duration of requests to the destination Alertmanager.",
			Buckets:                         prometheus.DefBuckets,
			NativeHistogramBucketFactor:     1.1,
			NativeHistogramMaxBucketNumber:  100,
			NativeHistogramMinResetDuration: time.Hour,
		}),
		retries: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "prometheus_alertmanager_write_retries_total",
			Help: "Total number of retried destination requests.",
		}),
		droppedAlerts: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "prometheus_alertmanager_write_dropped_alerts_total",
			Help: "Total number of alerts rejected or abandoned by reason.",
		}, []string{"reason"}),
		expiredFiringAlerts: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "prometheus_alertmanager_write_expired_firing_alerts_total",
			Help: "Total number of firing alerts whose refresh state expired without a resolved update.",
		}),
		queueLength: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "prometheus_alertmanager_write_queue_length",
			Help: "Current number of alerts in the in-memory queue.",
		}),
		trackedFiringAlerts: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "prometheus_alertmanager_write_tracked_firing_alerts",
			Help: "Current number of firing alerts retained for refresh.",
		}),
		trackedResolvedAlerts: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "prometheus_alertmanager_write_tracked_resolved_alerts",
			Help: "Current number of resolved alerts retained for repeated delivery.",
		}),
	}

	m.receivedAlerts = util.MustRegisterOrGet(reg, m.receivedAlerts).(prometheus.Counter)
	m.sentAlerts = util.MustRegisterOrGet(reg, m.sentAlerts).(prometheus.Counter)
	m.httpRequests = util.MustRegisterOrGet(reg, m.httpRequests).(prometheus.Counter)
	m.httpRequestFailures = util.MustRegisterOrGet(reg, m.httpRequestFailures).(*prometheus.CounterVec)
	m.httpRequestDuration = util.MustRegisterOrGet(reg, m.httpRequestDuration).(prometheus.Histogram)
	m.retries = util.MustRegisterOrGet(reg, m.retries).(prometheus.Counter)
	m.droppedAlerts = util.MustRegisterOrGet(reg, m.droppedAlerts).(*prometheus.CounterVec)
	m.expiredFiringAlerts = util.MustRegisterOrGet(reg, m.expiredFiringAlerts).(prometheus.Counter)
	m.queueLength = util.MustRegisterOrGet(reg, m.queueLength).(prometheus.Gauge)
	m.trackedFiringAlerts = util.MustRegisterOrGet(reg, m.trackedFiringAlerts).(prometheus.Gauge)
	m.trackedResolvedAlerts = util.MustRegisterOrGet(reg, m.trackedResolvedAlerts).(prometheus.Gauge)
	return m
}
