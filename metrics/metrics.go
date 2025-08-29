package metrics

import (
    "github.com/prometheus/client_golang/prometheus"
)

var (
    // Replication metrics
    ReplicationMessagesTotal = prometheus.NewCounterVec(
        prometheus.CounterOpts{
            Namespace: "arctic_mirror",
            Subsystem: "replication",
            Name:      "messages_total",
            Help:      "Total number of logical replication messages processed by type.",
        },
        []string{"type"},
    )

    ReplicationErrorsTotal = prometheus.NewCounter(
        prometheus.CounterOpts{
            Namespace: "arctic_mirror",
            Subsystem: "replication",
            Name:      "errors_total",
            Help:      "Total number of replication errors.",
        },
    )

    ReplicationLagBytes = prometheus.NewGauge(
        prometheus.GaugeOpts{
            Namespace: "arctic_mirror",
            Subsystem: "replication",
            Name:      "lag_bytes",
            Help:      "Approximate replication lag in bytes (server WAL end - client position).",
        },
    )

    ReplicationLastLSN = prometheus.NewGauge(
        prometheus.GaugeOpts{
            Namespace: "arctic_mirror",
            Subsystem: "replication",
            Name:      "last_lsn",
            Help:      "Last processed LSN as a 64-bit integer value.",
        },
    )

    // Proxy metrics
    ProxyQueriesTotal = prometheus.NewCounter(
        prometheus.CounterOpts{
            Namespace: "arctic_mirror",
            Subsystem: "proxy",
            Name:      "queries_total",
            Help:      "Total number of queries handled by the proxy.",
        },
    )

    ProxyQueryDurationSeconds = prometheus.NewHistogram(
        prometheus.HistogramOpts{
            Namespace: "arctic_mirror",
            Subsystem: "proxy",
            Name:      "query_duration_seconds",
            Help:      "Query execution duration in seconds.",
            Buckets:   prometheus.DefBuckets,
        },
    )
)

// Init registers all application metrics with the default Prometheus registry.
func Init() {
    prometheus.MustRegister(
        ReplicationMessagesTotal,
        ReplicationErrorsTotal,
        ReplicationLagBytes,
        ReplicationLastLSN,
        ProxyQueriesTotal,
        ProxyQueryDurationSeconds,
    )
}

