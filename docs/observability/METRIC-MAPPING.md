# Metric Mapping — Confluent Platform / CFK / Kubernetes

Verified live against this cluster on 2026-08-17: every row below was checked either via
`oc exec <pod> -- curl -s localhost:7778/metrics` (raw JMX-exporter output, `# HELP` line
quoted verbatim) or via Prometheus's own `/api/v1/query` (for Kubernetes-sourced metrics).
Nothing here is copied from a generic internet dashboard or assumed from JMX documentation
alone — every "Actual Prometheus Metric" column is something this cluster is really
emitting right now, and every "Available?" classification reflects what was actually
observed, not what Confluent's docs say *should* exist.

## CFK Prometheus configuration — how metrics collection is actually wired

**Finding: none of the six CFK CRs in `base/confluent-platform/` set `spec.metrics` at
all.** Grep across every CR (`kafka.yaml`, `kraft.yaml`, `connect.yaml`,
`schemaregistry.yaml`, `restproxy.yaml`, `controlcenter.yaml`) for `metrics:` /
`prometheus` / `jolokia` / `7778` turns up nothing — there is no explicit
`spec.metrics.jmx` or Prometheus-rule override anywhere in this repo.

That does **not** mean metrics are disabled. `oc explain kafka.spec.metrics` on this
cluster's CFK CRD (v1beta1) shows the field only has three sub-fields:

```
metrics.authentication   — auth for the metrics endpoints
metrics.jmx              — JMX access-control config
metrics.jolokia          — Jolokia access-control config
```

**There is no `spec.metrics.prometheus.rules` field in this CFK version's CRD at all** —
unlike the generic community JMX-exporter Helm chart, CFK 3.3.0 does not expose a
user-overridable Prometheus mapping-rule block. The JMX→Prometheus translation is baked
into the operator/image and is **on by default** — confirmed by inspecting a live pod:

```
$ oc get pod kafka-0 -n confluent -o jsonpath='{.spec.containers[0].ports}'
kafka: 9092(external) 9071(internal) 9072(replication) 9074(controller)
       7203(jmx) 7777(jolokia) 7778(prometheus)

$ oc get pod kafka-0 -n confluent -o jsonpath='{.metadata.annotations}'
prometheus.io/port: "7778"
prometheus.io/scrape: "true"
```

All three ports (7203 JMX, 7777 Jolokia, 7778 Prometheus) and the `prometheus.io/scrape`
annotation are injected by CFK automatically on every Confluent component pod in this
deployment (kafka, kraftcontroller, connect, schemaregistry, controlcenter,
kafkarestproxy) — nobody in this repo turned this on, and there is no CFK-native way to
customize *which* MBeans get mapped to *which* Prometheus metric names. This repo's
ServiceMonitors (`base/observability/servicemonitors/`) simply point Prometheus at that
already-running port 7778; no separate JMX-exporter sidecar was added, and none is
needed. **Conclusion: preserve as-is — there is nothing to configure or duplicate.**

## Classification key

| Code | Meaning |
|---|---|
| A | Available and currently exported — confirmed present in the live `/metrics` output, used in a dashboard panel. |
| A* | Available and currently exported, but **not yet used in any panel** — a genuine opportunity for a future addition, listed here so it isn't lost. |
| B | Documented Confluent/Kafka metric, but **not currently exported** by this deployment's JMX-exporter rule set — confirmed absent from the raw `/metrics` output (not a scrape/network problem, the rule mapping simply doesn't include it). |
| C | Client-side only — requires a producer/consumer application's own JMX, not broker telemetry. |
| D | Connector-specific — depends on which connector is actually deployed. |
| E | Kubernetes source — kubelet/cAdvisor/kube-state-metrics, not a Confluent component metric. |
| F | Flink source — Flink's own Prometheus reporter. |
| G | Log/event only — no reliable Prometheus metric exists for this condition in this deployment. |
| H | Not available / not verified for this component's current configuration (e.g. no connector of that type is deployed). |

---

## Kafka Broker / Cluster

| Functional metric | MBean : attribute | Actual Prometheus metric | Labels | Class | Dashboard |
|---|---|---|---|---|---|
| ActiveControllerCount | `kafka.controller:type=KafkaController,name=ActiveControllerCount` | `kafka_controller_kafkacontroller_value{name="ActiveControllerCount"}` | job, pod | A | 01, 02 |
| OfflinePartitionsCount | `kafka.controller:type=KafkaController,name=OfflinePartitionsCount` | `kafka_controller_kafkacontroller_value{name="OfflinePartitionsCount"}` | job, pod | A | 00, 01, 02 |
| UnderReplicatedPartitions | `kafka.server:type=ReplicaManager,name=UnderReplicatedPartitions` | `kafka_server_replicamanager_value{name="UnderReplicatedPartitions"}` | job, pod | A | 01, 02 |
| UnderMinIsrPartitionCount | `kafka.server:type=ReplicaManager,name=UnderMinIsrPartitionCount` | `kafka_server_replicamanager_value{name="UnderMinIsrPartitionCount"}` | job, pod | A* (confirmed present, not yet panelled) | — |
| AtMinIsrPartitionCount | `kafka.server:type=ReplicaManager,name=AtMinIsrPartitionCount` | `kafka_server_replicamanager_value{name="AtMinIsrPartitionCount"}` | job, pod | A* | — |
| UncleanLeaderElectionsPerSec | `kafka.controller:type=ControllerStats,name=UncleanLeaderElectionsPerSec` | `kafka_controller_controllerstats_oneminuterate{name="UncleanLeaderElectionsPerSec"}` | job, pod | A | 02 |
| BytesInPerSec / BytesOutPerSec | `kafka.server:type=BrokerTopicMetrics,name={BytesInPerSec,BytesOutPerSec}` | `kafka_server_brokertopicmetrics_oneminuterate{name=...}` (and `_count` for the raw counter) | name, topic, pod | A | 00,01,02,03 |
| BytesRejectedPerSec | `kafka.server:type=BrokerTopicMetrics,name=BytesRejectedPerSec` | `kafka_server_brokertopicmetrics_oneminuterate{name="BytesRejectedPerSec"}` | pod | A* | — |
| MessagesInPerSec | `kafka.server:type=BrokerTopicMetrics,name=MessagesInPerSec` | `kafka_server_brokertopicmetrics_count{name="MessagesInPerSec"}` | topic, pod | A | 01 |
| FailedProduceRequestsPerSec / FailedFetchRequestsPerSec | `kafka.server:type=BrokerTopicMetrics,name={FailedProduceRequestsPerSec,FailedFetchRequestsPerSec}` | `kafka_server_brokertopicmetrics_oneminuterate{name=...}` | pod | A* | — |
| Produce/Fetch request latency (RequestQueueTimeMs / LocalTimeMs / ResponseQueueTimeMs / TotalTimeMs) | `kafka.network:type=RequestMetrics,name={RequestQueueTimeMs,LocalTimeMs,ResponseQueueTimeMs,TotalTimeMs},request={Produce,FetchConsumer,...}` | `kafka_network_requestmetrics_mean{name=..., request=...}` | name, request, pod | A | 00, 01, 02 |
| RequestQueueSize | `kafka.network:type=RequestChannel,name=RequestQueueSize` | `kafka_network_requestchannel_value{name="RequestQueueSize"}` | pod | A | 02 |
| NetworkProcessorAvgIdlePercent | `kafka.network:type=SocketServer,name=NetworkProcessorAvgIdlePercent` | `kafka_network_socketserver_value{name="NetworkProcessorAvgIdlePercent"}` | pod | A | 02 |
| RequestHandlerAvgIdlePercent | `kafka.server:type=KafkaRequestHandlerPool,name=RequestHandlerAvgIdlePercent` | `kafka_server_kafkarequesthandlerpool_oneminuterate{name="RequestHandlerAvgIdlePercent"}` | pod | A | 02 |
| LeaderCount / PartitionCount | `kafka.server:type=ReplicaManager,name={LeaderCount,PartitionCount}` | `kafka_server_replicamanager_value{name=...}` | pod | A | 02 |
| IsrShrinksPerSec / IsrExpandsPerSec | `kafka.server:type=ReplicaManager,name={IsrShrinksPerSec,IsrExpandsPerSec}` | `kafka_server_replicamanager_oneminuterate{name=...}` | pod | A | 02 |
| ReplicaFetcherManager (TotalFetchRate, MaxLag, MinFetchRate) | `kafka.server:type=ReplicaFetcherManager,name=...` | `kafka_server_replicafetchermanager_value{name=...}` | pod, clientId | A | 02 |
| connection-count / connection-creation-rate / connection-close-rate | `kafka.server:type=socket-server-metrics,attribute={connection-count,connection-creation-rate,connection-close-rate}` | `kafka_server_socket_server_metrics_connection_{count,creation_rate,close_rate}` | listener, pod | A | 02 |

## Kafka Quotas / Throttling

| Functional metric | MBean : attribute | Actual Prometheus metric | Class | Dashboard |
|---|---|---|---|---|
| Per-broker Produce quota | `kafka.server:type=Produce,attribute=broker-quota` | `kafka_server_produce_broker_quota` | A | 01 |
| Per-broker Fetch quota | `kafka.server:type=Fetch,attribute=broker-quota` | `kafka_server_fetch_broker_quota` | A* | — |
| Per-broker Request/RequestRate quota | `kafka.server:type={Request,RequestRate},attribute=broker-quota` | `kafka_server_{request,requestrate}_broker_quota` | A* | — |
| Per-client-id throttle-time / byte-rate | `kafka.server:type={Produce,Fetch},client-id=<id>,attribute={throttle-time,byte-rate}` | not found in this deployment's `/metrics` output | B — only the aggregate *broker* quota gauge is exported here, not a per-client-id throttle-time series | — |

**Note:** what's exported is the broker's own aggregate quota gauge, not a per-client
throttle-time histogram. A per-client quota-violation panel would need client-id label
support in the exporter rule, which isn't present in this CFK version's default mapping —
documented as B, not faked.

## Kafka KRaft / Controller

| Functional metric | MBean : attribute | Actual Prometheus metric | Class | Dashboard |
|---|---|---|---|---|
| Current leader | `kafka.server:type=raft-metrics,attribute=current-leader` | `kafka_server_raft_metrics_current_leader` | A | 01, 02 |
| Current epoch | `kafka.server:type=raft-metrics,attribute=current-epoch` | `kafka_server_raft_metrics_current_epoch` | A | 01, 02 |
| High watermark | `kafka.server:type=raft-metrics,attribute=high-watermark` | `kafka_server_raft_metrics_high_watermark` | A | 02 |
| Log end offset / log end epoch | `kafka.server:type=raft-metrics,attribute={log-end-offset,log-end-epoch}` | `kafka_server_raft_metrics_log_end_{offset,epoch}` | A* | — |
| Number of voters / observers | `kafka.server:type=raft-metrics,attribute={number-of-voters,number-of-observers}` | `kafka_server_raft_metrics_number_of_{voters,observers}` | A* | — |
| Election latency (avg/max) | `kafka.server:type=raft-metrics,attribute=election-latency-{avg,max}` | `kafka_server_raft_metrics_election_latency_{avg,max}` | A* | — |
| Commit latency (avg/max) | `kafka.server:type=raft-metrics,attribute=commit-latency-{avg,max}` | `kafka_server_raft_metrics_commit_latency_{avg,max}` | A* | — |
| Append/fetch records rate | `kafka.server:type=raft-metrics,attribute={append-records-rate,fetch-records-rate}` | `kafka_server_raft_metrics_{append,fetch}_records_rate` | A | 01, 02 |
| Poll idle ratio | `kafka.server:type=raft-metrics,attribute=poll-idle-ratio-avg` | `kafka_server_raft_metrics_poll_idle_ratio_avg` | A* | — |
| Follower per-voter lag (fetch-timeout / follower-specific lag) | not found under this exact attribute name | — | B — only aggregate quorum metrics (high-watermark, log-end-offset) are exported; there's no per-follower lag breakdown to compute "how far behind is voter 2" directly. Cross-referencing `current-leader`+`high-watermark` across all three kraftcontroller pods (already possible via `by (pod)`) is the closest proxy. | — |
| Fenced brokers | `kafka.controller:type=KafkaController,name=FencedBrokerCount` | `kafka_controller_kafkacontroller_value{name="FencedBrokerCount", job="kraftcontroller"}` | A | 01 |
| BrokerRegistrationState | (documented KIP-500 concept) | not found in `/metrics` | B | — |
| metadata-load-error-count / metadata-apply-error-count | (documented) | not found in `/metrics` | B | — |
| Quorum state / current-vote | `kafka.server:type=raft-metrics,attribute=current-vote` | `kafka_server_raft_metrics_current_vote` present, but not decoded into a friendly panel here | A* | — |

**KRaft, not ZooKeeper**: confirmed this deployment exposes `raft-metrics` (KIP-595/630
KRaft MBeans) — there is no `zookeeper.*` metric family anywhere in the raw dump, and no
dashboard here uses ZooKeeper-era assumptions (no `SessionExpireListener`, no
`kafka.server:type=SessionExpireListener` etc.).

## Kafka JVM (all six Confluent components share this pattern)

| Functional metric | Source | Actual Prometheus metric | Class |
|---|---|---|---|
| Heap used / committed / max | `java.lang:type=Memory,attribute=HeapMemoryUsage` | `java_lang_memory_heapmemoryusage_{used,committed,max}` | A |
| Non-heap used | `java.lang:type=Memory,attribute=NonHeapMemoryUsage` | `java_lang_memory_nonheapmemoryusage_used` | A |
| Metaspace | JVM MXBean → generic jmx_exporter default rules | `jvm_memory_pool_bytes_used{pool="Metaspace"}` | A |
| Buffer pools / direct memory | `java.nio:type=BufferPool,name=direct` | `jvm_buffer_pool_used_bytes{pool="direct"}` | A |
| GC count / time (all collectors, not split young/old by name) | `java.lang:type=GarbageCollector,name=<collector>` | `jvm_gc_collection_seconds_{count,sum}{gc="<collector>"}` | A — collector name (e.g. G1 Young/Old Generation) is a label, not baked into the metric name, so "young vs old GC" is a `gc=` label filter, not two separate metrics |
| Thread count | `java.lang:type=Threading,attribute=ThreadCount` | `java_lang_threading_threadcount` / `jvm_threads_current` | A |
| Loaded classes | `java.lang:type=ClassLoading` | `jvm_classes_loaded` | A |
| Process CPU | `java.lang:type=OperatingSystem,attribute=ProcessCpuLoad` | `java_lang_operatingsystem_processcpuload` (also cross-checked via `process_cpu_seconds_total`) | A |
| Open/max file descriptors | `java.lang:type=OperatingSystem,attribute={OpenFileDescriptorCount,MaxFileDescriptorCount}` | `process_open_fds` / `process_max_fds` | A |

## Kafka Connect

| Functional metric | MBean : attribute | Actual Prometheus metric | Class |
|---|---|---|---|
| Connector class/type/version/status | `kafka.connect:type=connector-metrics,connector=<c>,attribute={connector-class,connector-type,connector-version,status}` | **not found** — zero matching series or HELP lines in the live `/metrics` output for job=connect | B — documented Connect MBean, silently dropped by this exporter's rule set. Connector state is only visible via the Connect REST API (`/connectors/<name>/status`), not Prometheus, in this deployment. |
| Connect->Kafka authentication success/failure (incl. reauth, handshake, connection-level) | `kafka.connect:type=connect-metrics,client-id=<id>,attribute={successful,failed}-{authentication,reauthentication,connection-authentications,connection,handshake}-{rate,total}` | `kafka_connect_connect_metrics_{successful,failed}_{authentication,reauthentication,connection_authentications,connection,handshake}_{rate,total}` | A — found during a cross-check against a user-supplied Datadog JMX scraper reference; confirmed live (5 successful, 0 failed authentications) and added to Dashboard 2's Kafka Connect row. Security-relevant given this pipeline is Kerberos-authenticated end to end. |
| Last error timestamp, per connector/task | `kafka.connect:type=task-error-metrics,attribute=last-error-timestamp` | `kafka_connect_task_error_metrics_last_error_timestamp` | A — found during the same cross-check; added as a "time since last error" panel. |
| Connect->Kafka connection creation/close rate | `kafka.connect:type=connect-metrics,attribute=connection-{creation,close}-rate` | `kafka_connect_connect_metrics_connection_{creation,close}_rate` | A — added; previously only `connection_count` (a gauge, not the creation/close churn rate) was dashboarded. |
| Coordinator: assigned connectors/tasks, failed rebalances | `kafka.connect:type=connect-coordinator-metrics,attribute={assigned-connectors,assigned-tasks,failed-rebalance-total}` | `kafka_connect_connect_coordinator_metrics_{assigned_connectors,assigned_tasks,failed_rebalance_total}` | A — distinct from the `connect-worker-rebalance-metrics` family already dashboarded (epoch/rebalancing/completed-rebalances); `failed_rebalance_total` is a genuinely separate counter (confirmed live at 1) that was invisible before. Added. |
| Task pause-ratio / running-ratio | `kafka.connect:type=connector-task-metrics,attribute={pause-ratio,running-ratio}` | `kafka_connect_connector_task_metrics_{pause_ratio,running_ratio}` | A |
| Offset commit success/failure % and avg/max time | `kafka.connect:type=connector-task-metrics,attribute=offset-commit-*` | `kafka_connect_connector_task_metrics_offset_commit_*` | A |
| Batch size avg/max | `kafka.connect:type=connector-task-metrics,attribute=batch-size-{avg,max}` | `kafka_connect_connector_task_metrics_batch_size_{avg,max}` | A |
| Worker connector-count / task-count | `kafka.connect:type=connect-worker-metrics,attribute={connector-count,task-count}` | `kafka_connect_connect_worker_metrics_{connector,task}_count` | A |
| connector-startup-{attempts,success,failure}-total and -percentage | `kafka.connect:type=connect-worker-metrics,attribute=connector-startup-*` | `kafka_connect_connect_worker_metrics_connector_startup_*` | A (all six documented startup metrics confirmed present) |
| task-startup-{attempts,success,failure}-total and -percentage | same, task-startup-* | `kafka_connect_connect_worker_metrics_task_startup_*` | A |
| Rebalancing flag / completed-rebalances-total | `kafka.connect:type=connect-worker-rebalance-metrics,attribute={rebalancing,completed-rebalances-total}` | `kafka_connect_connect_worker_rebalance_metrics_{rebalancing,completed_rebalances_total}` | A |
| epoch | `kafka.connect:type=connect-worker-rebalance-metrics,attribute=epoch` | `kafka_connect_connect_worker_rebalance_metrics_epoch` | A |
| leader-name (which worker is group leader) | (documented distributed-worker concept) | not found as its own metric — only `epoch`/`rebalancing` are exposed | B — leader identity is only visible via the Connect REST API, not a labeled Prometheus gauge, in this deployment |
| Rebalance avg/max time | `kafka.connect:type=connect-worker-rebalance-metrics,attribute=rebalance-{avg,max}-time-ms` | `kafka_connect_connect_worker_rebalance_metrics_rebalance_{avg,max}_time_ms` | A |
| **Source task**: source-record-poll/write-rate, active-count (avg/max), poll-batch time (avg/max) | `kafka.connect:type=source-task-metrics,connector=<c>,task=<t>` | `kafka_connect_source_task_metrics_*` | A — 27 series confirmed live, backing the JDBC source connector reading from SQL Server |
| **Sink task**: sink-record-read/send-rate, put-batch time, offset-commit-* | `kafka.connect:type=sink-task-metrics,connector=<c>,task=<t>` | **zero series** for job=connect right now | H — not a mapping gap: this repo currently runs a JDBC **source** connector only (SQL Server → Kafka); Flink is the consumer, not a Connect sink connector. Would become A the moment a real sink connector is deployed. |
| total-errors-logged / total-record-errors / total-record-failures / total-records-skipped / total-retries / deadletterqueue-produce-{requests,failures} | `kafka.connect:type=task-error-metrics,connector=<c>,task=<t>` | `kafka_connect_task_error_metrics_total_*`, `kafka_connect_task_error_metrics_deadletterqueue_produce_*` | A — all seven documented error attributes confirmed present |

## Schema Registry

| Functional metric | MBean : attribute | Actual Prometheus metric | Class |
|---|---|---|---|
| master-slave-role | `kafka.schema.registry:type=master-slave-role,attribute=master-slave-role` | `kafka_schema_registry_master_slave_role_master_slave_role` | A |
| Jetty connections-active / accepted-rate / opened-rate / closed-rate | `schemaregistry_confluent:type=jetty-metrics,attribute=connections-*` | `schemaregistry_confluent_jetty_metrics_connections_{active,accepted_rate,opened_rate,closed_rate}` | A — corrected below; the pre-existing 10-schema-registry.json dashboard's "Active Connections" panel was actually querying `kafka_schema_registry_kafka_schema_registry_metrics_connection_count` (SR's own Kafka-client connection count), not this jetty/REST metric. Fixed as part of this verification pass: that panel was retitled to make clear it's the Kafka-client view, and a new panel using the real jetty metrics below was added for the actual REST-API-facing signal. |
| busy_thread_count / thread_pool_usage / request_queue_size | `schemaregistry_confluent:type=jetty-metrics,attribute={busy-thread-count,thread-pool-usage,request-queue-size}` | `schemaregistry_confluent_jetty_metrics_{busy_thread_count,thread_pool_usage,request_queue_size}` | A — confirmed present and non-zero live (`busy_thread_count=1`, `thread_pool_usage=0.005`, `request_queue_size=0`); added to 10-schema-registry.json as a new panel in this pass |
| Per-endpoint jersey metrics (subjects.list, subjects.versions.register, schemas.ids.get-schema, etc.) | `kafka.schema.registry:type=jersey-metrics,attribute=<endpoint>.request-rate` etc. | `schemaregistry_confluent_jersey_metrics_<endpoint>_request_rate` (endpoint baked into the metric **name**, not a label — e.g. `schemaregistry_confluent_jersey_metrics_metadata_version_request_latency_95`) | A — confirmed real, but **each REST endpoint is its own metric name**, not a label value, so a single "SR request rate" panel can't `sum()` across endpoints without a fragile `{__name__=~...}` regex (tested live: this produces a Prometheus `execution: vector cannot contain metrics with the same labelset` error). Dashboard 2's SR row deliberately uses the aggregate Kafka-client-side `kafka_schema_registry_kafka_schema_registry_metrics_request_rate` instead, and per-endpoint breakdowns are left to 10-schema-registry.json. |
| Overall aggregate request-rate (SR's own Kafka client, used for its `_schemas` topic) | `kafka.schema.registry:type=kafka-schema-registry-metrics,attribute=request-rate` | `kafka_schema_registry_kafka_schema_registry_metrics_request_rate` | A |

## Kafka REST Proxy / Control Center (same rest-utils framework)

| Functional metric | MBean : attribute | Actual Prometheus metric | Class |
|---|---|---|---|
| Per-endpoint request rate/error/latency | `io.confluent.kafkarest:type=jersey-metrics,attribute=<endpoint>.*` | `kafkarestproxy_confluent_jersey_metrics_<endpoint>_*` — same per-endpoint-in-name pattern as SR | A for endpoints that have real traffic (confirmed: `topics_list_v2` has live non-zero series); most of the other ~1,870 series exist as metric *definitions* but read 0/NaN because that specific REST endpoint has never been called in this lab |
| Control Center cluster-offline healthcheck | `io.confluent.rest:type=healthcheck,attribute=cluster-offline` | `rest_utils_healthcheck_cluster_offline` | A |
| Control Center volume usage | (CaaS-specific, not a standard Kafka/Connect MBean) | `io_confluent_caas_volumemetrics_{used,total,percentused,percentavailable}` | A |

## Kubernetes / OpenShift (Dashboard 1)

| Functional metric | Source | Actual Prometheus metric | Class |
|---|---|---|---|
| Pod phase, restarts, owner | kube-state-metrics | `kube_pod_status_phase`, `kube_pod_container_status_restarts_total`, `kube_pod_owner` | E |
| Pod/container CPU, memory | kubelet cAdvisor (`/metrics/cadvisor`) | `container_cpu_usage_seconds_total`, `container_memory_working_set_bytes` | E |
| Requests/limits | kube-state-metrics | `kube_pod_container_resource_{requests,limits}` | E |
| Node conditions, allocatable | kube-state-metrics | `kube_node_status_condition`, `kube_node_status_allocatable` | E |
| StatefulSet/Deployment replica health | kube-state-metrics | `kube_statefulset_{replicas,status_replicas_ready}`, `kube_deployment_status_replicas_ready` | E |
| PVC used/capacity | kubelet volume stats | `kubelet_volume_stats_{used_bytes,capacity_bytes}` | E |
| Network RX/TX, errors, drops | kubelet cAdvisor | `container_network_{receive,transmit}_bytes_total`, `container_network_receive_{errors,packets_dropped}_total` | E |
| Node-level disk IOPS/latency, node CPU from `/proc` directly | node-exporter | **not available** — no node-exporter is deployed in this single-node CRC lab (deliberately trimmed for resource budget, see `apps/observability-app.yaml`) | H — documented, not faked; Dashboard 1 derives node CPU/memory pressure from the *sum of container requests/usage* against `kube_node_status_allocatable` instead |
| Prometheus's own scrape/rule health | Prometheus self-metrics | `up`, `scrape_duration_seconds`, `prometheus_rule_evaluation_failures_total`, `prometheus_tsdb_head_series` | A |

## Apache Flink (Dashboards 2, 3, and the existing 24-flink-mvp.json)

| Functional metric | Source | Actual Prometheus metric | Class |
|---|---|---|---|
| Job status, uptime, restarts, checkpoints | Flink's native Prometheus reporter (`flink-conf.yaml` `metrics.reporter.prom.factory.class`) | `flink_jobmanager_job_{uptime,numRestarts,fullRestarts,numberOfCompletedCheckpoints,numberOfFailedCheckpoints,lastCheckpointSize,...}` | F — confirmed 417 live `flink_*` series; this repo's Flink job (`statefarm-claims-processor`) is actually running with real telemetry, contrary to an earlier session's note that no Flink job existed yet |
| Per-task throughput, busy/backpressure time | Flink TaskManager reporter | `flink_taskmanager_job_task_{numRecordsInPerSecond,numRecordsOutPerSecond,busyTimeMsPerSecond,backPressuredTimeMsPerSecond}` | F |
| Flink's embedded Kafka source/sink client metrics | Flink's Kafka connector re-exports the underlying Kafka client's own JMX/metrics through Flink's reporter | `flink_taskmanager_job_task_operator_KafkaSourceReader_KafkaConsumer_records_lag_max`, `flink_taskmanager_job_task_operator_KafkaProducer_*` | C — this is genuinely **client-side** telemetry (Flink's internal consumer/producer), correctly distinguished in Dashboard 3 from broker-side signals |

## Client-side vs. broker-side — explicit distinction (per the "do not confuse" rule)

- **Broker-side, safe to treat as ground truth**: everything under `kafka_server_*`,
  `kafka_network_*`, `kafka_controller_*`, `kafka_cluster_*`, `kafka_log_*` — these come
  from the brokers/controllers themselves and are used throughout Dashboards 1 and 2.
- **Client-side, requires the specific client to be JMX-instrumented**:
  `kafka_consumer_consumer_fetch_manager_metrics_records_lag_max` (used in Dashboard 3) is
  the **broker's own internal consumers** (license consumer, replica fetchers, etc.), not
  an application consumer group — documented explicitly in that panel's description.
  `flink_taskmanager_job_task_operator_KafkaSourceReader_KafkaConsumer_*` and
  `...KafkaProducer_*` are Flink's own embedded client metrics — real, but scoped to
  Flink's Kafka I/O only, not a general-purpose producer/consumer fleet view.
- **No true per-consumer-group lag breakdown exists** in this deployment. The closest
  real signals are (a) the client-side lag above and (b) the broker's aggregate
  `kafka_coordinator_group_consumer_lag_emitter_*` percentile histogram (cluster-wide, not
  per-group). A Burrow or `kafka-lag-exporter` deployment would be required for genuine
  per-consumer-group/per-partition lag, and is **not implemented here** — documented as a
  gap in Dashboard 3's own panel description rather than approximated with a misleading
  substitute.
- **No producer-client telemetry** (record-send-rate, record-error-rate, batch-size,
  buffer-pool-wait-time from an actual *application* producer) is available for the SQL
  Server → Kafka pipeline, because that pipeline is a Kafka Connect source connector, not
  a hand-written producer app — Connect's own `source-task-metrics` (poll/write rate,
  batch time) are the correct and only available proxy, and that's what Dashboard 3 uses.
