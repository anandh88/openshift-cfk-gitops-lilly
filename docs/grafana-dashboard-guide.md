# Grafana dashboard guide

Access: `oc port-forward -n monitoring svc/observability-grafana 3000:80`,
then `http://localhost:3000`. Credentials: `admin` / whatever's in the
`grafana-admin` secret (`oc get secret grafana-admin -n monitoring -o
jsonpath='{.data.admin-password}' | base64 -d`).

All dashboards below are auto-provisioned via ConfigMaps labeled
`grafana_dashboard: "1"` (the chart's sidecar picks them up automatically
- no manual JSON import, ever, per this repo's GitOps convention). Source
files: `base/observability/grafana/dashboards/*.json`, wrapped as
ConfigMaps in `base/observability/grafana/dashboard-configmaps/`.

As of this pass, the Kafka-side and Connect-side dashboards were each
consolidated from several fragmented dashboards into one "platform"
dashboard apiece (per explicit request), adapted from Confluent's own
`confluentinc/jmx-monitoring-stacks` reference dashboards rather than a
generic community one, with pie charts/gauges added and several
previously-unused-but-real metric families folded in. See each dashboard's
own `description` field for the full list of what was ported, fixed, or
dropped (and why) - that's kept in the dashboard JSON itself so it can't
drift out of sync with this doc.

## 01 - Streaming Platform Overview

The NOC/SRE landing page. Answers "is the platform healthy?" in one
screen: brokers up, active controllers, offline/under-replicated
partitions, Connect worker/task health, cluster throughput, and CPU/memory
for Kafka and Connect. No Flink or SQL Server panels - see
`docs/observability-architecture.md` for why.

## 02 - Kafka Platform (KRaft + Brokers + Topics)

One consolidated dashboard (formerly split across 02/03/04/07/10) covering
the whole Kafka side: platform health stats + partition-leadership and
listener-connection pie charts, per-topic log-size/partition-count pie
charts and per-topic throughput, the KRaft controller's quorum/raft/event-
queue internals, broker request rate and per-request-type latency
breakdown, throughput/system/JVM, thread utilization, ISR/replica lag,
connections, group coordinator, and transaction-coordinator/quota metrics
(real, currently idle/unlimited since EOS and client quotas aren't in use
here). Adapted from Confluent's own `kafka-cluster-kraft.json`/`kraft.json`/
`kafka-topics-kraft.json` reference dashboards (`confluentinc/jmx-monitoring-
stacks`) - the KRaft variants, since this cluster has no ZooKeeper.

## 08 - Connect Platform

One consolidated dashboard (formerly split across 08/09) covering Connect
worker health (connectors/tasks running/failed/paused, rebalancing,
coordinator join/sync/heartbeat health, per-broker-node client latency),
per-task CPU/memory load and startup success/failure rates, task error
metrics, and the **Kerberos JDBC Source Connector's own** poll rate, write
rate, active-record count, and batch timing - the direct answer to "how
many records is my JDBC Source reading?"

## 22 - Kubernetes/OpenShift Resources

CPU/memory by pod, container restarts, pod phase counts (Pending/Failed/
Running), PVC usage %, and network throughput - scoped to this platform's
own namespaces (`confluent`, `monitoring`, `flink-*`, `cmf-operator`), not
the whole shared CRC cluster.

## 23 - JVM Deep Dive

Generic JVM dashboard (heap, non-heap, threads, CPU load, file
descriptors) covering every JMX-Prometheus-exporter-scraped pod in
`confluent` at once - Kafka, KRaft, Connect, Schema Registry, REST Proxy,
Control Center all show up as separate series, filterable by pod.

## Bonus: chart-provided Kubernetes dashboards

`kube-prometheus-stack` ships its own set of generic Kubernetes dashboards
(Compute Resources by Cluster/Namespace/Pod/Workload, Networking,
Persistent Volumes, Kubelet, Prometheus Overview) - these came free with
the Helm chart and are visible in Grafana alongside the 4 above. Useful
for cross-checking the custom "22 - Kubernetes/OpenShift Resources"
dashboard against a different view of the same underlying metrics.

## What each dashboard does NOT answer yet, and why

See `docs/observability-architecture.md`'s "What's deferred, and why"
section - Consumer Groups, dedicated KRaft/Schema-Registry/REST-Proxy/
Control-Center views, dedicated Produce/Fetch, full Platform Capacity, all
five Flink dashboards, and SQL Server are all named there with the
specific reason each is deferred, not silently missing.
