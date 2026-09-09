package http

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/grafana/alloy/internal/util"
)

type metrics struct {
	receivedAlerts      prometheus.Counter
	transformationError prometheus.Counter
	sentAlerts          prometheus.Counter
	httpRequests        prometheus.Counter
	httpFailures        *prometheus.CounterVec
	httpDuration        prometheus.Histogram
	retries             prometheus.Counter
	droppedAlerts       *prometheus.CounterVec
	queueLength         prometheus.Gauge
}

func newMetrics(reg prometheus.Registerer) *metrics {
	m := &metrics{
		receivedAlerts:      prometheus.NewCounter(prometheus.CounterOpts{Name: "prometheus_alertmanager_http_received_alerts_total", Help: "Total alerts accepted from upstream Alloy components."}),
		transformationError: prometheus.NewCounter(prometheus.CounterOpts{Name: "prometheus_alertmanager_http_transformation_errors_total", Help: "Total alerts rejected because transformation failed."}),
		sentAlerts:          prometheus.NewCounter(prometheus.CounterOpts{Name: "prometheus_alertmanager_http_sent_alerts_total", Help: "Total transformed alerts accepted by the HTTP destination."}),
		httpRequests:        prometheus.NewCounter(prometheus.CounterOpts{Name: "prometheus_alertmanager_http_requests_total", Help: "Total JSON HTTP requests sent."}),
		httpFailures:        prometheus.NewCounterVec(prometheus.CounterOpts{Name: "prometheus_alertmanager_http_request_failures_total", Help: "Failed JSON HTTP requests by reason."}, []string{"reason"}),
		httpDuration:        prometheus.NewHistogram(prometheus.HistogramOpts{Name: "prometheus_alertmanager_http_request_duration_seconds", Help: "Duration of JSON HTTP requests.", Buckets: prometheus.DefBuckets, NativeHistogramBucketFactor: 1.1, NativeHistogramMaxBucketNumber: 100, NativeHistogramMinResetDuration: time.Hour}),
		retries:             prometheus.NewCounter(prometheus.CounterOpts{Name: "prometheus_alertmanager_http_retries_total", Help: "Total retried JSON HTTP requests."}),
		droppedAlerts:       prometheus.NewCounterVec(prometheus.CounterOpts{Name: "prometheus_alertmanager_http_dropped_alerts_total", Help: "Alerts rejected or abandoned by reason."}, []string{"reason"}),
		queueLength:         prometheus.NewGauge(prometheus.GaugeOpts{Name: "prometheus_alertmanager_http_queue_length", Help: "Current number of transformed JSON bodies in the queue."}),
	}
	m.receivedAlerts = util.MustRegisterOrGet(reg, m.receivedAlerts).(prometheus.Counter)
	m.transformationError = util.MustRegisterOrGet(reg, m.transformationError).(prometheus.Counter)
	m.sentAlerts = util.MustRegisterOrGet(reg, m.sentAlerts).(prometheus.Counter)
	m.httpRequests = util.MustRegisterOrGet(reg, m.httpRequests).(prometheus.Counter)
	m.httpFailures = util.MustRegisterOrGet(reg, m.httpFailures).(*prometheus.CounterVec)
	m.httpDuration = util.MustRegisterOrGet(reg, m.httpDuration).(prometheus.Histogram)
	m.retries = util.MustRegisterOrGet(reg, m.retries).(prometheus.Counter)
	m.droppedAlerts = util.MustRegisterOrGet(reg, m.droppedAlerts).(*prometheus.CounterVec)
	m.queueLength = util.MustRegisterOrGet(reg, m.queueLength).(prometheus.Gauge)
	return m
}
