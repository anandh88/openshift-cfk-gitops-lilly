# Prometheus metrics guide

How to find out what Prometheus is actually receiving, and the one
non-obvious quirk of this specific setup that will otherwise cost you an
hour of confused PromQL.

## The one thing you need to know first

The JMX-Prometheus-exporter running on every CFK component's port 7778
uses **generic MBean-to-metric conversion**, not curated per-metric names.
That means the real metric identity usually lives in a `name` **label**,
not baked into the metric name itself:

```promql
# WRONG - this metric name doesn't exist
kafka_server_underreplicatedpartitions

# RIGHT - the metric is generic, the identity is a label
kafka_server_replicamanager_value{name="UnderReplicatedPartitions"}
```

This applies to `kafka_server_replicamanager_value`,
`kafka_controller_kafkacontroller_value`,
`kafka_network_requestmetrics_*`, `kafka_server_brokertopicmetrics_count`,
and similar. Always check the label set before assuming a metric doesn't
exist.

## Discover before you write PromQL - the actual commands used

Every metric in this repo's dashboards/rules was found this way, not
guessed:

```bash
# Port-forward to the deployed Prometheus
oc port-forward -n monitoring svc/observability-prometheus 9090:9090

# 1. List every metric name currently being scraped
curl -s http://localhost:9090/api/v1/label/__name__/values | python3 -m json.tool

# 2. For a generic-conversion metric, find its real label values
curl -s http://localhost:9090/api/v1/query --data-urlencode \
  'query=kafka_server_replicamanager_value' | python3 -m json.tool
# -> look at the "name" label on each returned series

# 3. Confirm a specific metric+label combo actually has data
curl -s http://localhost:9090/api/v1/query --data-urlencode \
  'query=kafka_server_replicamanager_value{name="UnderReplicatedPartitions"}'
```

Or use `scripts/list-platform-metrics.sh <kafka|connect|schemaregistry|restproxy|jvm|infra|flink|all>`
for a pre-filtered view of the same thing.

## Real metric reference, by component

### Kafka broker (`job="kafka"`)

| What you want | Query |
|---|---|
| Under-replicated partitions | `kafka_server_replicamanager_value{name="UnderReplicatedPartitions"}` |
| Under-min-ISR partitions | `kafka_server_replicamanager_value{name="UnderMinIsrPartitionCount"}` |
| Leader count | `kafka_server_replicamanager_value{name="LeaderCount"}` |
| Partition count | `kafka_server_replicamanager_value{name="PartitionCount"}` |
| Offline partitions (cluster-wide) | `kafka_controller_kafkacontroller_value{name="OfflinePartitionsCount"}` |
| Active controller count | `kafka_controller_kafkacontroller_value{name="ActiveControllerCount"}` |
| Bytes in/out per sec | `kafka_server_brokertopicmetrics_count{name="BytesInPerSec"\|"BytesOutPerSec"}` (has a `topic` label too) |
| Messages in per sec | `kafka_server_brokertopicmetrics_count{name="MessagesInPerSec"}` |
| Produce/fetch request rate | `kafka_server_brokertopicmetrics_count{name="TotalProduceRequestsPerSec"\|"TotalFetchRequestsPerSec"}` |
| Failed produce/fetch requests | `kafka_server_brokertopicmetrics_count{name="FailedProduceRequestsPerSec"\|"FailedFetchRequestsPerSec"}` |
| Request latency (p99, ms) | `kafka_network_requestmetrics_99thpercentile{name="TotalTimeMs", request="Produce"\|"FetchConsumer"\|"FetchFollower"}` (has a `request` label for the request type) |
| Request rate/errors | `kafka_network_requestmetrics_count{name="RequestsPerSec"\|"ErrorsPerSec", request="..."}` |

### Kafka Connect (`job="connect"`)

| What you want | Query |
|---|---|
| Connectors configured | `kafka_connect_connect_worker_metrics_connector_count` |
| Tasks running/failed/paused | `kafka_connect_connect_worker_metrics_connector_{running,failed,paused}_task_count` |
| Source connector poll rate | `kafka_connect_source_task_metrics_source_record_poll_rate` (this is the JDBC Source Connector's actual read rate) |
| Source connector write rate | `kafka_connect_source_task_metrics_source_record_write_rate` |
| Task record errors | `kafka_connect_task_error_metrics_total_record_errors` |
| Worker rebalance state/time | `kafka_connect_connect_worker_rebalance_metrics_{rebalancing,rebalance_avg_time_ms}` |

### Consumer lag (real, but currently sparse in this environment)

`kafka_consumer_consumer_fetch_manager_metrics_records_lag` /
`_lag_max` / `_lag_avg` are the real per-partition lag metrics, exposed by
whichever pod runs a Kafka consumer client. In this environment that's
only relevant if a sink connector or a real consuming application exists
- the JDBC Source Connector doesn't consume. Control Center exposes
`rest_utils_consumer_group_total_lag{groupId="..."}` for groups it's
actively tracking (its own internal groups, or ones a user has recently
browsed in the UI) - also real, but dynamic/on-demand rather than a
standing metric for every group. Use
`scripts/generate-observability-load.sh start --lag` to populate real,
non-trivial lag data on demand.

### JVM (any component on port 7778)

| What you want | Query |
|---|---|
| Heap used/max | `java_lang_memory_heapmemoryusage_{used,max}` |
| Non-heap used | `java_lang_memory_nonheapmemoryusage_used` |
| Thread count | `java_lang_threading_threadcount` |
| Process CPU load | `java_lang_operatingsystem_processcpuload` |
| Open file descriptors | `java_lang_operatingsystem_openfiledescriptorcount` |

### Kubernetes/OpenShift infra (kube-state-metrics + kubelet, standard names)

| What you want | Query |
|---|---|
| Container CPU | `container_cpu_usage_seconds_total` |
| Container memory (working set) | `container_memory_working_set_bytes` |
| Pod restarts | `kube_pod_container_status_restarts_total` |
| OOMKilled | `kube_pod_container_status_last_terminated_reason{reason="OOMKilled"}` |
| PVC usage/capacity | `kubelet_volume_stats_used_bytes` / `kubelet_volume_stats_capacity_bytes` |
| Pod phase | `kube_pod_status_phase` |

### Flink - not available yet

No Prometheus metrics exist on this cluster for Flink - confirmed via
`scripts/list-platform-metrics.sh flink`. See
`docs/observability-architecture.md`'s "Known limitations" section for
what's needed first.
