# Observability Gap Analysis / Backlog

Tracks every P0/P1/P2 item from the three-layer observability architecture
(Layer 1 Kubernetes/OpenShift, Layer 2 CFK/Confluent components, Layer 3
Kafka/Connect/Flink workloads) against what this deployment actually
implements today, per the "MVP does not mean shallow" requirement — nothing
here is silently dropped; anything not implemented is listed with why and
what it would take.

Status key: **Done** (live and dashboarded) / **Partial** (some sub-items
done, others not) / **Blocked** (needs a decision or infra change — see
note) / **Not applicable** (genuinely doesn't apply to this deployment).

## P0 — must work in MVP

| Item | Status | Where | Notes |
|---|---|---|---|
| Node CPU/memory/disk/network, requests/limits/actual, throttling, OOM | Done | Dashboard 00 | No node-exporter (deliberately trimmed, single-node CRC) — CPU/mem derived from container-sum vs `kube_node_status_allocatable` instead of `node_cpu_seconds_total`; disk IOPS/latency genuinely unavailable without node-exporter (documented, not faked). CPU throttling: `container_cpu_cfs_throttled_seconds_total` **not yet added** — see below. |
| Node conditions | Done | Dashboard 00 | Ready/MemoryPressure/DiskPressure/PIDPressure/NetworkUnavailable via `kube_node_status_condition`. |
| Pod/container health | Done | Dashboard 00 | Status phase, restarts, joined pod table. |
| PVC health | Done | Dashboard 00 | Utilization %, growth trend, all 9 PVCs (Kafka, KRaft, C3, Prometheus). |
| OpenShift ClusterOperator health | **Blocked** | — | Requires the RBAC grant described under "Blocked items" below (ClusterOperator status is exposed via the Cluster Version Operator's own `/metrics`, which is kube-rbac-proxy-gated like the rest of the control plane). |
| Prometheus target health | Done | Dashboard 00 | `up`, scrape duration, rule-evaluation-failures row. |
| Kafka/KRaft/Connect/SR/Flink/CFK operator availability | Done | Dashboards 00 + 01 | StatefulSet/Deployment desired-vs-ready tables; per-component `up{job=...}`. |
| Replica convergence | Done | Dashboard 00 | `kube_statefulset_status_replicas_ready` / `kube_deployment_status_replicas_ready` table. |
| CPU/memory/JVM/GC/network per component | Done | Dashboard 01 collapsed rows | Kafka, KRaft, Connect, Schema Registry, JVM cross-component. |
| Kafka replication + request health | Done | Dashboard 01 "Kafka Broker & Cluster (full detail)" | URP, UnderMinIsr, AtMinIsr, offline partitions, ISR shrink/expand, full request-latency breakdown. |
| KRaft quorum health | Done | Dashboard 01 "KRaft / Metadata Quorum" | Leader, epoch, fenced brokers, voters, observers, log-end-offset, election/commit latency, MetadataErrorCount. |
| Connect worker/rebalance health | Done | Dashboard 01 "Kafka Connect (full detail)" | Completed + failed rebalances, epoch, assigned connectors/tasks, auth success/failure. |
| SR request/latency/error health | Done | Dashboard 01 "Schema Registry (full detail)" | Jersey per-endpoint + corrected Jetty REST-server panel. |
| Kafka topics/partitions/throughput | Done | Dashboard 03 | Bytes in/out and log size by topic. |
| Consumer lag from a valid source | Done (partial) | Dashboard 03 | Client-reported lag + broker aggregate percentile — **no true per-consumer-group breakdown** (documented gap, see METRIC-MAPPING.md; a kafka-lag-exporter deployment would close it, matching `jmx-monitoring-stacks`' own `kafka-lag-exporter.json` reference). |
| Connect connector/task flows/errors/retries | Done | Dashboards 01 + 03 | Source poll/write rate, poll-batch time, errors/retries/skipped/DLQ. |
| Flink records/backpressure/checkpoints/jobs/slots | Done | Dashboards 01 "Apache Flink (full detail)" + 03 | Full depth carried over from the pre-existing 24-flink-mvp build (checkpoints, backpressure, RocksDB, producer/consumer detail, skew). |

**P0 sub-items closed this pass:**
- **CPU throttling ratio per pod/container** — `container_cpu_cfs_throttled_seconds_total` isn't exposed by this kubelet's cAdvisor, but `container_cpu_cfs_throttled_periods_total` / `container_cpu_cfs_periods_total` are; added as a % panel to Dashboard 00's Pod CPU row.

**P0 sub-items still missing:**
- **ClusterOperator / ClusterVersion / MachineConfigPool status** — blocked on the RBAC grant below.

## P1 — add where the cluster exposes reliable metrics

| Item | Status | Notes |
|---|---|---|
| kube-apiserver availability/latency/errors | **Blocked** | See "Blocked items" below. |
| etcd leader/DB-size/fsync/commit latency | **Blocked** | Same RBAC gate as above, **plus** etcd's endpoint additionally resets the TCP connection on an unauthenticated request in a way that suggests mTLS client-cert enforcement on top of the bearer-token check — confirmed reachable at the network layer (`https://etcd.openshift-etcd.svc:9979`) but not yet confirmed accessible even with a valid token. Genuinely higher effort than apiserver/scheduler/CoreDNS/OVN. |
| kube-scheduler scheduling latency/pending pods | **Blocked** | Same RBAC gate. |
| CoreDNS request rate/latency/errors | **Blocked** | Confirmed reachable over HTTPS on port 9154 (not the upstream-default 9153) and gated by the same kube-rbac-proxy bearer-token check; a `dns-monitoring` ClusterRole already exists on this cluster specifically for this. |
| OVN-Kubernetes / CNI pod health, packet drops/errors | **Blocked** | Confirmed reachable (ports 9103 node / 9108 control-plane), same RBAC gate. |
| CSI controller/node health, PVC pending duration, attach/provision latency | Partial | PVC capacity/usage already covered (Layer 2). Attach/provision latency and CSI plugin health itself not evaluated this pass — lower priority than the control-plane items above; `crc-csi-hostpath-provisioner` doesn't appear to expose its own `/metrics` (not checked in depth). |
| cert-manager certificate expiry | **Done** | Just added — `certmanager_certificate_expiration_timestamp_seconds` confirmed live, no auth/NetworkPolicy barrier (unlike everything else in this table), new ServiceMonitor + Dashboard 00 panel. |
| Advanced storage latency (P95/P99 read/write) | Not implemented | No node-exporter, no CSI-side histogram metric found — would need node-exporter's `node_disk_*` histograms, explicitly out of scope for this trimmed single-node lab. |
| Client producer/consumer telemetry | Documented gap | Genuinely unavailable without instrumenting an actual producer/consumer application — correctly not fabricated anywhere (see METRIC-MAPPING.md's "Do not confuse broker and client metrics" section). |
| Cluster Linking / Schema Linking | Not applicable | Neither is configured in this repo (single cluster, no `_confluent-link-metadata` config beyond the metric name existing on the broker by default; no linked cluster to report on). |

## P2 — requires extra telemetry/integration

| Item | Status | Notes |
|---|---|---|
| Audit/RBAC/SCC denial visualization | Not implemented | Correctly identified as log/event-only (Classification G) — OpenShift audit logs, not a Prometheus metric. Would need a log pipeline (Loki, etc.), out of scope here. |
| True network bandwidth tests | Not implemented | No active-probe tooling deployed; byte-counter-derived "bandwidth" would misrepresent actual available bandwidth, correctly not faked. |
| Storage-backend pool capacity | Not implemented | `crc-csi-hostpath-provisioner` is a local hostPath-backed provisioner with no pool-capacity metric exposed distinct from node filesystem usage. |
| NUMA/topology-manager detail | Not applicable | CRC is a single VM with no NUMA topology exposed by the topology manager; correctly not fabricated. |
| Specialized hardware/OS metrics | Not applicable | No specialized hardware in this lab. |

## Blocked items — needs an explicit decision

Getting kube-apiserver, etcd, kube-scheduler, CoreDNS, and OVN-Kubernetes
metrics flowing requires binding the Prometheus ServiceAccount
(`observability-prometheus` in the `monitoring` namespace) to the
cluster's built-in `system:monitoring` ClusterRole:

```yaml
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRoleBinding
metadata:
  name: observability-prometheus-system-monitoring
subjects:
  - kind: ServiceAccount
    name: observability-prometheus
    namespace: monitoring
roleRef:
  kind: ClusterRole
  name: system:monitoring
  apiGroup: rbac.authorization.k8s.io
```

This is the same, standard ClusterRole OpenShift's own cluster-monitoring-operator
Prometheus uses — its rules only grant `get` on the `/metrics`, `/healthz`,
`/livez`, `/readyz` nonResourceURLs and `nodes/metrics`, nothing else (no
write access, no other resources). Confirmed live before stopping:

- CoreDNS (`https://<pod-ip>:9154/metrics`), OVN node (`:9103`), and OVN
  control-plane (`:9108`) all return `Unauthorized` without a token — the
  network path is open, only the RBAC check is missing.
- `kube-scheduler` (port 443) and `kube-apiserver`
  (`kubernetes.default.svc:443/metrics`) return Kubernetes API `Status:
  Failure` objects without a token, consistent with the same kube-rbac-proxy
  gate.
- etcd's endpoint reset the connection rather than returning `Unauthorized`,
  suggesting it may also require a client certificate — not yet confirmed
  even with a valid bearer token.

Creating a `ClusterRoleBinding` is a cluster-scoped change and was blocked
by the coding agent's own safety classifier when attempted directly, which
is the correct behavior — **this needs your explicit go-ahead** before it's
created. If approved, the next steps are: create the binding, add
ServiceMonitors for CoreDNS/OVN-node/OVN-control-plane/kube-scheduler/
kube-apiserver with `bearerTokenFile:
/var/run/secrets/kubernetes.io/serviceaccount/token` and
`tlsConfig.insecureSkipVerify: true` (these endpoints use the cluster's
internal serving certs, which Prometheus doesn't have the CA for), verify
`up{job=...}` for each, then build the actual Layer 1B dashboard row.
etcd would need a separate investigation into client-cert requirements
before it's worth attempting.

**Update: approved and implemented.** CoreDNS, kube-scheduler, kube-apiserver,
and OVN-Kubernetes (node) are now scraped and dashboarded in Dashboard 1's new
"OpenShift Control Plane & Network Fabric (Layer 1B)" row (request error
rate/P99 latency for the apiserver, pending pods and scheduling-attempt rate
for the scheduler, non-NOERROR response rate for CoreDNS, CNI setup
latency/workqueue depth for OVN). etcd remains excluded per the note above.

## Dashboard 3 deep-dive critique (user-supplied, cross-checked against
## current Confluent docs)

The user reviewed Dashboard 3 against current Confluent documentation and
raised 12 points. Verified and actioned:

| # | Item | Status |
|---|---|---|
| 1 | Native consumer-lag-emitter (`consumer-lag-offsets`) instead of Burrow/kafka-lag-exporter | **Done** — confirmed live, corrected in METRIC-MAPPING.md and rebuilt as Dashboard 3's primary lag view. This was a real factual correction, not a preference. |
| 9 | Connect error panel graphing raw counters instead of `rate()` | **Done** — fixed in both Dashboard 3 and the equivalent panel in Dashboard 2 (same bug existed in both, inherited from the original 08-connect-platform.json). |
| 10 | Flink checkpoint counters shown as lifetime totals, not windowed | **Done** — changed to `increase(...[$__range])`, scoped to the dashboard's selected time range instead of all-time. |

Reviewed and **not yet implemented** (real, valuable, but each a substantial
addition in its own right — listed here rather than silently dropped):

| # | Item | Scope note |
|---|---|---|
| 2 | Full chained variable set (environment/cluster/broker/partition/client-id/connect-cluster/task/flink-cluster/operator/subtask) across all three dashboards | This deployment is a single cluster/single environment, so `$environment`/`$cluster` would be constant-value variables (harmless to add, low value here — flagged for when this pattern is reused against a multi-cluster/multi-env Prometheus). `$broker`/`$partition`/`$client_id`/`$task`/`$operator`/`$subtask` are genuine, high-cardinality drill-down variables not yet wired in; `$consumer_group` was added this pass. |
| 3 | Explicit "Kafka Request Path" diagnostic row (client → request queue → handler → replication → response queue → network, each hop's time as its own panel) | The underlying metrics (RequestQueueTimeMs/LocalTimeMs/RemoteTimeMs/ResponseQueueTimeMs/ResponseSendTimeMs) are already dashboarded in Dashboard 2's "Kafka Broker & Cluster (full detail)" row (imported from the original 02-kafka-platform.json) but as separate per-request-type panels, not assembled into one hop-by-hop diagnostic flow. Worth a dedicated pass. |
| 4 | Explicit Kafka saturation row (idle %, queues, purgatory, connections, CPU throttling, GC, disk, PVC in one place) | Every individual metric is already dashboarded somewhere (Dashboard 1 has CPU throttling/PVC, Dashboard 2 has idle%/queues/GC); not yet consolidated into one "why is Kafka slow at 45% CPU" purpose-built row. |
| 5 | Broker Balance/Skew table (leaders, partitions, bytes in/out, disk, CPU per broker, one row per broker) | Not implemented — needs a `joinByField` table merging ~6 per-broker metrics, similar to Dashboard 1's pod table. Real, valuable, not yet built. |
| 6 | Dedicated Replication Health section with explicit correlation guidance (URP + follower lag + replication traffic + disk latency → storage bottleneck) | URP/ISR/offline partitions are dashboarded individually; `ReplicationBytesInPerSec`/`ReplicationBytesOutPerSec`/reassignment metrics not yet checked for availability in this deployment, and no correlation-guidance panel exists. |
| 7 | Quotas/throttling promoted to a first-class, prominent row, broken out by user/client-id | The broker-aggregate quota row exists (Dashboard 2); per-client throttle-time is available client-side (`kafka_producer_producer_metrics_produce_throttle_time_*`, `kafka_consumer_..._fetch_throttle_time_*`, found this pass, documented in METRIC-MAPPING.md) but not yet in a panel, and nothing is broken out "by user" (this cluster has no per-user quotas configured, confirmed earlier). |
| 8 | Explicit server/client/platform/connect/flink telemetry classification in METRIC-MAPPING.md | Partially satisfied — the "Client-side vs. broker-side" section already does this narratively; not reorganized into the exact 5-bucket table structure requested. |
| 11 | "Top Offenders" panels at every layer (top CPU/memory/restart/throttled pods, top topics/groups/connectors/operators by the relevant bad signal) | Not implemented anywhere yet. This is a real, broadly-applicable UX pattern (topk() queries) that would improve all three dashboards; flagged as the single highest-value next addition after the items above. |
| 12 | "Incident Correlation" row per major dashboard (latency/CPU/throttling/heap/GC/disk/network/queue/URP/lag/errors/backpressure on one shared time axis) | Not implemented. Would need one row per dashboard stacking ~8-10 already-dashboarded signals as thin sparkline-style panels sharing a time axis — mechanically straightforward given the metrics already exist, just not yet assembled this way. |

None of these were skipped because they're wrong — all are legitimate,
well-reasoned troubleshooting improvements. They're listed here, not
implemented in the same pass as the P0 corrections (native lag emitter,
PromQL rate/window bugs), because each is independently substantial and
this list should be worked through deliberately rather than rushed.
