# Observability runbook

Operational reference for the Phase 1 stack. See
`docs/observability-architecture.md` for design/decisions,
`docs/prometheus-metrics-guide.md` for real metric names, and
`docs/alerting-runbook.md` for what each alert means.

## First-time access

```bash
# Grafana
oc port-forward -n monitoring svc/observability-grafana 3000:80
open http://localhost:3000
# user: admin, password:
oc get secret grafana-admin -n monitoring -o jsonpath='{.data.admin-password}' | base64 -d

# Prometheus (for direct PromQL / targets / rules / alerts pages)
oc port-forward -n monitoring svc/observability-prometheus 9090:9090
open http://localhost:9090
```

## Validate the whole chain

```bash
./scripts/validate-observability.sh
```

`PASS`/`FAIL`/`WARN` output; non-zero exit only on a `FAIL`. Two `WARN`s
are expected and documented as environment-accurate, not bugs: no Flink
Prometheus endpoint, and 1/3 (not 3/3) Kafka brokers - see
`docs/observability-architecture.md`.

## Troubleshooting: "Grafana panel shows No Data"

Work down this chain - each step's fix is usually obvious once you know
which layer actually broke:

```
Grafana panel shows No Data
            |
            v
1. Check the datasource
   Grafana -> Connections -> Data sources -> Prometheus -> Test
            |
            v
2. Check the PromQL directly against Prometheus
   oc port-forward -n monitoring svc/observability-prometheus 9090:9090
   curl -s http://localhost:9090/api/v1/query --data-urlencode 'query=<the exact panel query>'
   -> empty result? go to step 3. Error/malformed? fix the query
      (see docs/prometheus-metrics-guide.md - likely a wrong "name" label)
            |
            v
3. Check the Prometheus target itself
   curl -s http://localhost:9090/api/v1/targets | python3 -c "
   import json,sys; d=json.load(sys.stdin)
   for t in d['data']['activeTargets']:
       print(t['labels'].get('job'), t['health'], t.get('lastError',''))
   "
   -> target missing entirely? go to step 4. Target shows 'down' with an
      error? that error tells you directly (usually a NetworkPolicy or
      connection refused)
            |
            v
4. Check the ServiceMonitor
   oc get servicemonitor -n monitoring
   oc get servicemonitor <name> -n monitoring -o yaml
   -> confirm spec.selector.matchLabels matches the TARGET SERVICE'S OWN
      metadata.labels (not its pod-selecting spec.selector - this exact
      mistake cost real debugging time when this stack was first built:
      CFK Services carry "type: kafka", not "app: kafka")
            |
            v
5. Check the Service
   oc get svc <name> -n confluent -o yaml
   -> does it have the port you expect, named correctly?
   oc get svc <name> -n confluent -o jsonpath='{.metadata.labels}'
   -> does it actually carry the label your ServiceMonitor selects on?
            |
            v
6. Check the metrics endpoint directly
   oc exec <pod> -n confluent -c <container> -- curl -s localhost:7778/metrics | head -20
   -> real Prometheus text? Endpoint down/refused? empty?
            |
            v
7. Check the JMX exporter/reporter itself
   (for CFK components, port 7778 is the JMX-Prometheus-exporter Java
   agent, always-on by default - if step 6 fails, this usually means the
   pod itself is unhealthy, not a metrics-specific problem)
            |
            v
8. Check application configuration
   Confirm nothing in base/confluent-platform/*.yaml's spec.metrics block
   (if ever added) is restricting/breaking the exporter - none of the six
   CRDs currently set spec.metrics at all, so this is the least likely
   cause today
```

## NetworkPolicy blocking a scrape: diagnosis commands

```bash
# Confirm the monitoring-scrape-confluent policy exists and looks right
oc get networkpolicy monitoring-scrape-confluent -n confluent -o yaml

# From inside a monitoring-namespace pod, test raw reachability
oc run netpol-test --rm -it --image=curlimages/curl -n monitoring --restart=Never -- \
  curl -sv --max-time 5 http://<pod-ip>:7778/metrics

# If that times out but the target's port/labels are correct, check the
# NetworkPolicy's namespaceSelector actually matches the monitoring
# namespace's own label:
oc get namespace monitoring -o jsonpath='{.metadata.labels}'
# should show kubernetes.io/metadata.name: monitoring
```

Same pattern as `docs/troubleshooting.md`'s general NetworkPolicy section
- these are the confluent-namespace-specific commands.

## Re-running after changes

Standard GitOps flow, same as the rest of this repo - commit+push, then
force a sync rather than relying purely on the automated poll interval
while iterating:

```bash
git add base/observability/ apps/observability-app.yaml
git commit -m "..."
git push
oc patch application observability-config -n argocd \
  -p '{"metadata":{"annotations":{"argocd.argoproj.io/refresh":"hard"}}}' --type=merge
# for chart-values changes (apps/observability-app.yaml's helm.values block):
oc patch application observability -n argocd \
  -p '{"metadata":{"annotations":{"argocd.argoproj.io/refresh":"hard"}}}' --type=merge
oc patch application observability -n argocd --type=merge \
  -p '{"operation":{"sync":{"revision":"88.2.0","prune":true}}}'
```

**Known Helm-values gotcha** (cost real debugging time building this
stack): setting a chart's `securityContext:` value to `null` to clear a
hardcoded non-root UID works under a local `helm template` run, but Argo
CD's own Helm value-merge does **not** treat `null` the same way - it
leaves the chart's default intact. Use an explicit UID within the target
namespace's allocated SCC range instead
(`oc get namespace <ns> -o jsonpath='{.metadata.annotations}'` ->
`openshift.io/sa.scc.uid-range`), as done in `apps/observability-app.yaml`.

## Generating load for demos

```bash
./scripts/generate-observability-load.sh start          # producer only
./scripts/generate-observability-load.sh start --lag     # + slow consumer for lag demo
./scripts/generate-observability-load.sh stop
```

Runs entirely via `oc exec` into `kafka-0` against a dedicated
`observability-load-test` topic - never touches `sqlserver-Claims` or the
Kerberos JDBC Source Connector.

## Checking what Prometheus actually has

```bash
./scripts/list-platform-metrics.sh kafka
./scripts/list-platform-metrics.sh connect
./scripts/list-platform-metrics.sh all
```
