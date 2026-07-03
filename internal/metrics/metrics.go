package metrics

import "github.com/prometheus/client_golang/prometheus/promauto"
import "github.com/prometheus/client_golang/prometheus"

var (
	HTTPRequestsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "http_requests_total",
		Help: "Total HTTP requests by method, path and status code.",
	}, []string{"method", "path", "status"})

	HTTPRequestDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "http_request_duration_seconds",
		Help:    "HTTP request duration in seconds.",
		Buckets: prometheus.DefBuckets,
	}, []string{"method", "path"})

	// OutboxPending is set by the outbox worker every tick.
	OutboxPending = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "outbox_pending_total",
		Help: "Number of unsent rows in the outbox table.",
	})

	KafkaEventsProcessed = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "kafka_events_processed_total",
		Help: "Total Kafka events by result: success | dlq.",
	}, []string{"result"})
)
