# Grafana dashboard guide

Access: `oc port-forward -n monitoring svc/observability-grafana 3000:80`,
then `http://localhost:3000`. Credentials: `admin` / whatever's in the
`grafana-admin` secret (`oc get secret grafana-admin -n monitoring -o
jsonpath='{.data.admin-password}' | base64 -d`).

All 7 dashboards below are auto-provisioned via ConfigMaps labeled
`grafana_dashboard: "1"` (the chart's sidecar picks them up automatically
- no manual JSON import, ever, per this repo's GitOps convention). Source
files: `base/observability/grafana/dashboards/*.json`, wrapped as
ConfigMaps in `base/observability/grafana/dashboard-configmaps/`.

## 01 - Streaming Platform Overview

The NOC/SRE landing page. Answers "is the platform healthy?" in one
screen: brokers up, active controllers, offline/under-replicated
partitions, Connect worker/task health, cluster throughput, and CPU/memory
for Kafka and Connect. No Flink or SQL Server panels - see
`docs/observability-architecture.md` for why.

## 02 - Kafka Cluster Overview

Cluster-wide throughput, replication health, and produce/fetch request
rates per broker.

## 03 - Kafka Broker Deep Dive

Per-broker leader/partition counts, request-handler idle %, produce/fetch
p99 latency, CPU/memory, and disk usage (`data0-kafka-*` PVCs).

## 04 - Kafka Topics

Per-topic throughput - top 10 by bytes in/out and messages in. Cardinality
note: one series per (topic × broker) - see
`docs/observability-cardinality.md`. Run
`scripts/generate-observability-load.sh start` to see this dashboard react
to real traffic on a dedicated `observability-load-test` topic (doesn't
touch `sqlserver-Claims` or the JDBC connector).

## 08-09 - Kafka Connect Overview & Connector Deep Dive

Worker health (connectors configured, tasks running/failed/paused,
rebalancing state) plus the **Kerberos JDBC Source Connector's own** poll
rate, write rate, and active-record count - the direct answer to "how many
records is my JDBC Source reading?" Also task error rate and worker JVM
heap.

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
the Helm chart and are visible in Grafana alongside the 7 above. Useful
for cross-checking the custom "22 - Kubernetes/OpenShift Resources"
dashboard against a different view of the same underlying metrics.

## What each dashboard does NOT answer yet, and why

See `docs/observability-architecture.md`'s "What's deferred, and why"
section - Consumer Groups, dedicated KRaft/Schema-Registry/REST-Proxy/
Control-Center views, dedicated Produce/Fetch, full Platform Capacity, all
five Flink dashboards, and SQL Server are all named there with the
specific reason each is deferred, not silently missing.
