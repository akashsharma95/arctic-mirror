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

    // Compaction metrics
    CompactionRunsTotal = prometheus.NewCounter(
        prometheus.CounterOpts{
            Namespace: "arctic_mirror",
            Subsystem: "compaction",
            Name:      "runs_total",
            Help:      "Total number of compaction runs.",
        },
    )

    CompactionDurationSeconds = prometheus.NewHistogram(
        prometheus.HistogramOpts{
            Namespace: "arctic_mirror",
            Subsystem: "compaction",
            Name:      "duration_seconds",
            Help:      "Duration of compaction runs in seconds.",
            Buckets:   prometheus.DefBuckets,
        },
    )

    CompactionFilesProcessedTotal = prometheus.NewCounter(
        prometheus.CounterOpts{
            Namespace: "arctic_mirror",
            Subsystem: "compaction",
            Name:      "files_processed_total",
            Help:      "Total number of files processed by compaction.",
        },
    )

    CompactionFilesCompactedTotal = prometheus.NewCounter(
        prometheus.CounterOpts{
            Namespace: "arctic_mirror",
            Subsystem: "compaction",
            Name:      "files_compacted_total",
            Help:      "Total number of files compacted.",
        },
    )

    CompactionBytesSavedTotal = prometheus.NewCounter(
        prometheus.CounterOpts{
            Namespace: "arctic_mirror",
            Subsystem: "compaction",
            Name:      "bytes_saved_total",
            Help:      "Total number of bytes saved by compaction.",
        },
    )

    CompactionErrorsTotal = prometheus.NewCounter(
        prometheus.CounterOpts{
            Namespace: "arctic_mirror",
            Subsystem: "compaction",
            Name:      "errors_total",
            Help:      "Total number of compaction errors.",
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
        CompactionRunsTotal,
        CompactionDurationSeconds,
        CompactionFilesProcessedTotal,
        CompactionFilesCompactedTotal,
        CompactionBytesSavedTotal,
        CompactionErrorsTotal,
    )
}

