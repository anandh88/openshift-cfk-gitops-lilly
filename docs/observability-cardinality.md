# Observability cardinality

Cardinality as a production concern, not an afterthought - what labels
this stack actually emits, which ones grow with your data (not just your
infrastructure), and what to watch if this pattern moves to a real
production cluster with thousands of topics.

## Labels used, by metric family

| Metric family | Labels | Grows with |
|---|---|---|
| `kafka_server_brokertopicmetrics_count{name=...}` | `topic`, `pod`, `job` | **topic count** - one series per (topic × broker × metric name) |
| `kafka_network_requestmetrics_*{name=...}` | `request` (Produce/FetchConsumer/FetchFollower/...), `pod` | request-type count (bounded, ~10-15 values) × broker |
| `kafka_server_replicamanager_value{name=...}` | `pod` | broker count only - no topic/partition dimension |
| `kafka_controller_kafkacontroller_value{name=...}` | `pod` | broker count only |
| `kafka_connect_source_task_metrics_*` | `pod`, task-level identity | connector task count |
| `kafka_consumer_consumer_fetch_manager_metrics_records_lag` | `client-id`, `topic`, `partition` | **partition count × consumer count** - the most dangerous one in this list |
| `container_cpu_usage_seconds_total` / `container_memory_working_set_bytes` | `namespace`, `pod`, `container` | pod count |
| `kube_pod_container_status_restarts_total` | `namespace`, `pod`, `container` | pod count |
| `kubelet_volume_stats_*` | `namespace`, `persistentvolumeclaim` | PVC count |

## The dangerous one: per-partition consumer lag

`kafka_consumer_consumer_fetch_manager_metrics_records_lag` is
per-(client-id × topic × partition). A single consumer group reading a
50-partition topic already produces 50 series just for that one metric,
times however many consumer groups exist. In this local environment
that's a non-issue (near-zero consuming applications), but it's the
metric to budget for first in a real production rollout - either accept
the cardinality (it's usually the single most valuable metric for
SRE work, worth the cost) or pre-aggregate via a recording rule
(`sum by (topic) (...)` collapses the partition dimension if per-partition
detail isn't needed for alerting, only for occasional drill-down via a
direct query).

## Topic-level cardinality

`kafka_server_brokertopicmetrics_count{topic="..."}` exists per (topic ×
broker), for every `name` value (BytesInPerSec, MessagesInPerSec, etc. -
about a dozen `name` values with a `topic` dimension). On this local
cluster (a handful of topics), that's negligible. At production scale
(thousands of topics), this is the second thing to budget for - the "04 -
Kafka Topics" dashboard's `topk(10, ...)` queries are deliberately written
to avoid ever rendering all-topics-at-once in a panel, but the underlying
series still exist in Prometheus's TSDB regardless of whether a dashboard
renders them all.

## What this stack does NOT do (kept low-cardinality on purpose)

- **No per-partition broker-side replica metrics** - `LeaderCount`/
  `PartitionCount` on `kafka_server_replicamanager_value` are per-broker
  totals, not broken out by partition. Real per-partition ISR/leader
  detail would need a different metric source (not currently scraped) and
  would multiply cardinality by partition count across every topic.
- **No per-client-id producer metrics** - Kafka brokers don't expose
  individual producer application identity in broker-side metrics at all
  (see the Producer Visibility distinction below); this stack only has
  broker-side aggregate produce metrics, never fabricated per-producer
  labels.
- **`ruleNamespaceSelector`/`serviceMonitorNamespaceSelector` scoped to
  `monitoring` only** - deliberately not cluster-wide, to avoid this
  Prometheus instance evaluating rules or discovering targets across
  namespaces this platform doesn't own (see
  `docs/observability-architecture.md`'s note on the ~49 OpenShift-internal
  rule groups that still leak through via the operator's own cluster-wide
  RBAC - a known, documented gap, not something cardinality-managed here).

## Broker-side vs. client-instrumented producer telemetry

Per the request's own distinction: everything in this stack's "Produce"
panels is **broker-side** (`kafka_server_brokertopicmetrics_count`,
`kafka_network_requestmetrics_*`) - aggregate produce request/byte/error
rates as seen by the broker, with no per-producer-application identity,
because Kafka brokers genuinely don't track that. To get true
per-producer-application telemetry (e.g. "app X's producer is retrying"),
the producing application itself needs its own JMX/OpenTelemetry/
Micrometer instrumentation, scraped as a separate target with its own
`job`/`app` label - not something this stack fabricates today, since no
such instrumented producer application exists in this environment yet
(the JDBC Source Connector's producer-side metrics ARE available via
`kafka_connect_source_task_metrics_source_record_write_rate`, which is
the closest thing to "client producer telemetry" currently wired up).
