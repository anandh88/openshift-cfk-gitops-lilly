# Alerting runbook

Every alert defined in `base/observability/rules/{kafka,connect,infra}-rules.yaml`,
what it means, and what to actually do about it. Alertmanager isn't wired
to a real notification channel yet (see `docs/observability-architecture.md`)
- check firing alerts via Prometheus's own Alerts page
(`oc port-forward -n monitoring svc/observability-prometheus 9090:9090`,
then `/alerts`) or Grafana's Alerting view.

## Kafka alerts (`kafka-rules.yaml`)

**KafkaBrokerDown** (critical) - `up{job=~"kafka|kraftcontroller"} == 0`
for 2m. Prometheus can't scrape the broker/controller at all. Check
`oc get pods -n confluent -l app=kafka` first - if the pod itself is
`Running`, this is a NetworkPolicy or port issue, not a broker crash.

**KafkaOfflinePartitions** (critical) - any partition has no leader at
all; produce/consume for that partition is fully down. Check `oc logs` on
the active controller for the actual cause (usually all replicas for that
partition are down simultaneously).

**KafkaUnderReplicatedPartitions** (warning, 5m) - replicas falling
behind their leader. On a healthy multi-broker cluster this means a
broker is struggling (disk I/O, network, GC pause) - check that broker's
CPU/memory panel in "02 - Kafka Platform" first.

**KafkaUnderMinISR** (critical, 5m) - producers using `acks=all` will
start failing outright for these partitions. More urgent than plain URP.

**KafkaNoActiveController** (critical) - `ActiveControllerCount` summed
across the cluster isn't exactly 1. Either metadata operations have
stalled (0 controllers) or there's a split-brain (2+) - both need
immediate attention.

**KafkaHighRequestLatency** (warning, 5m) - Produce or FetchConsumer p99
above 1s. Cross-check "02 - Kafka Platform"'s CPU/request-handler-
idle panels - this is almost always a saturation symptom, not a network
issue.

**KafkaDiskUsageHigh** / **KafkaDiskUsageCritical** (80%/90% of the
`data0-kafka-*` PVC) - check `log.retention.hours` (currently `168` per
`base/confluent-platform/kafka.yaml`) and whether retention is actually
being enforced, before just growing the PVC.

**KRaftControllerUnavailable** / **SchemaRegistryDown** - same pattern as
`KafkaBrokerDown`, different component.

## Connect alerts (`connect-rules.yaml`)

**KafkaConnectWorkerDown** (critical) - this is the same `connect-0`
worker running the Kerberos JDBC Source Connector. If this fires, the
whole SQL Server pipeline is down, not just monitoring. See
`docs/kerberos-runbook.md` for connector-specific troubleshooting once
the worker itself is confirmed up.

**KafkaConnectorFailed** (critical) - a task has actually failed (not
just paused). `curl -sk https://connect.apps-crc.testing/connectors/<name>/status`
for the real error trace - this alert only tells you *that* it failed,
not *why*.

**KafkaConnectorTaskFailed** (warning) - fires on any increase in
`total_record_failures` over 5m, even if the task is still technically
running (e.g. a few bad records going to a DLQ). Less urgent than the
above, but worth checking `docs/kerberos-runbook.md`'s
`Unable to obtain password from user` section if it's the JDBC connector.

**KafkaConnectNoRunningTasks** (critical, 3m) - tasks are configured but
none are actually RUNNING (all paused/failed/unassigned). No records are
flowing even though the connector object itself might show as configured.

## Infra alerts (`infra-rules.yaml`)

All scoped to this platform's own namespaces (`confluent`, `monitoring`,
`flink-*`, `cmf-operator`) - not the whole shared CRC cluster, which also
runs OpenShift's own control plane that isn't this platform's to alert on.

**PodCrashLooping** (warning) - >3 restarts in 15m for one container.
**ContainerOOMKilled** (critical) - fires immediately, no `for:` delay,
since this is never a transient blip. **HighCPU** / **HighMemory**
(warning, 10m sustained) - thresholds (1.5 cores / 8Gi) are deliberately
generous for this environment; tune down for a smaller/tighter deployment.
**PodPending** (warning, 10m) - usually a resource-headroom or SCC/
scheduling problem on this specific CRC node (see
`docs/observability-architecture.md`'s node-headroom notes) - `oc describe
pod` first. **PVCAlmostFull** (warning, 80%) - same pattern as the
Kafka-specific disk alert, generalized to any PVC in `confluent`/`monitoring`.

## General troubleshooting

If an alert fires that doesn't match what you just fixed, check
`docs/observability-runbook.md`'s "Grafana panel shows No Data" flow in
reverse - the same target/ServiceMonitor/NetworkPolicy chain that feeds a
dashboard panel also feeds the alert's underlying metric.
