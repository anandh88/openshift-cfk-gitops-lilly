#!/usr/bin/env bash
# End-to-end health check for the Phase 1 observability stack (Prometheus +
# Grafana + ServiceMonitors + PrometheusRules). Read-only throughout.
# See docs/observability-runbook.md for what to do when something here FAILs.
set -uo pipefail

pass() { echo "  PASS: $1"; }
fail() { echo "  FAIL: $1"; ((FAILURES++)); }
warn() { echo "  WARN: $1"; }

FAILURES=0
PROM_PF_PORT="${PROM_PF_PORT:-19200}"
GRAFANA_PF_PORT="${GRAFANA_PF_PORT:-19201}"

cleanup() { jobs -p | xargs -r kill 2>/dev/null; }
trap cleanup EXIT

echo "=== 1. Namespace ==="
oc get namespace monitoring >/dev/null 2>&1 && pass "monitoring namespace exists" || fail "monitoring namespace missing"

echo
echo "=== 2. Prometheus running ==="
oc get statefulset prometheus-observability-prometheus -n monitoring >/dev/null 2>&1 \
  && pass "Prometheus StatefulSet exists" || fail "Prometheus StatefulSet missing"
READY=$(oc get pod prometheus-observability-prometheus-0 -n monitoring -o jsonpath='{.status.containerStatuses[?(@.name=="prometheus")].ready}' 2>/dev/null)
[[ "${READY}" == "true" ]] && pass "Prometheus container ready" || fail "Prometheus container not ready"

echo
echo "=== 3. Grafana running ==="
READY=$(oc get pod -n monitoring -l app.kubernetes.io/name=grafana -o jsonpath='{.items[0].status.containerStatuses[?(@.name=="grafana")].ready}' 2>/dev/null)
[[ "${READY}" == "true" ]] && pass "Grafana container ready" || fail "Grafana container not ready"

echo
echo "=== 4-16. Live checks via port-forward ==="
oc port-forward -n monitoring svc/observability-prometheus "${PROM_PF_PORT}:9090" >/tmp/validate-obs-prom-pf.log 2>&1 &
PROM_PID=$!
oc port-forward -n monitoring svc/observability-grafana "${GRAFANA_PF_PORT}:80" >/tmp/validate-obs-grafana-pf.log 2>&1 &
GRAFANA_PID=$!
sleep 4

# 4. Prometheus datasource in Grafana
ADMIN_PASS="$(oc get secret grafana-admin -n monitoring -o jsonpath='{.data.admin-password}' 2>/dev/null | base64 -d)"
DS="$(curl -s -u "admin:${ADMIN_PASS}" "http://localhost:${GRAFANA_PF_PORT}/api/datasources" 2>/dev/null | python3 -c "import json,sys; d=json.load(sys.stdin); print(len([x for x in d if x.get('type')=='prometheus']))" 2>/dev/null)"
[[ "${DS}" -ge 1 ]] 2>/dev/null && pass "Grafana has a Prometheus datasource" || fail "Grafana has no Prometheus datasource"

# 5-10. Metrics endpoints for each CFK component, via Prometheus's own target
# health. Plain-array pairs instead of an associative array - macOS ships
# bash 3.2 by default (no `declare -A` support), and this needs to run
# there without requiring a homebrew bash install.
JOB_LIST="kafka kraftcontroller connect schemaregistry kafkarestproxy controlcenter"
JOB_LABEL_kafka="Kafka"
JOB_LABEL_kraftcontroller="KRaft"
JOB_LABEL_connect="Connect"
JOB_LABEL_schemaregistry="Schema Registry"
JOB_LABEL_kafkarestproxy="REST Proxy"
JOB_LABEL_controlcenter="Control Center"
TARGETS_JSON="$(curl -s "http://localhost:${PROM_PF_PORT}/api/v1/targets" 2>/dev/null)"
for job in ${JOB_LIST}; do
  label_var="JOB_LABEL_${job}"
  label="${!label_var}"
  HEALTH="$(echo "${TARGETS_JSON}" | python3 -c "
import json,sys
d=json.load(sys.stdin)
for t in d['data']['activeTargets']:
    if t['labels'].get('job')=='${job}':
        print(t['health']); break
" 2>/dev/null)"
  if [[ "${HEALTH}" == "up" ]]; then
    pass "${label} (${job}) target is up"
  else
    fail "${label} (${job}) target is '${HEALTH:-missing}', not up"
  fi
done

# 11. Flink Operator endpoint - documented as NOT exposing Prometheus metrics
warn "Flink Kubernetes Operator has no Prometheus endpoint on this cluster (confirmed: port 8085 is plain-text health-check only) - see docs/observability-architecture.md"

# 12. ServiceMonitors
SM_COUNT="$(oc get servicemonitor -n monitoring -l release=observability --no-headers 2>/dev/null | wc -l | tr -d ' ')"
[[ "${SM_COUNT}" -ge 6 ]] 2>/dev/null && pass "${SM_COUNT} ServiceMonitors found" || fail "expected >=6 ServiceMonitors, found ${SM_COUNT}"

# 13. All Prometheus targets up
DOWN_COUNT="$(echo "${TARGETS_JSON}" | python3 -c "
import json,sys
d=json.load(sys.stdin)
print(sum(1 for t in d['data']['activeTargets'] if t['health']!='up'))
" 2>/dev/null)"
[[ "${DOWN_COUNT}" -eq 0 ]] 2>/dev/null && pass "all Prometheus targets are up" || warn "${DOWN_COUNT} target(s) not up - check /targets in the Prometheus UI"

# 14. Grafana dashboards
DASH_COUNT="$(curl -s -u "admin:${ADMIN_PASS}" "http://localhost:${GRAFANA_PF_PORT}/api/search?type=dash-db" 2>/dev/null | python3 -c "import json,sys; print(len(json.load(sys.stdin)))" 2>/dev/null)"
[[ "${DASH_COUNT}" -ge 7 ]] 2>/dev/null && pass "${DASH_COUNT} Grafana dashboards provisioned" || fail "expected >=7 dashboards, found ${DASH_COUNT:-0}"

# 15. Kafka brokers count (informational - see docs/observability-architecture.md
#     for why this CRC cluster runs 1, not 3)
BROKER_COUNT="$(echo "${TARGETS_JSON}" | python3 -c "
import json,sys
d=json.load(sys.stdin)
print(sum(1 for t in d['data']['activeTargets'] if t['labels'].get('job')=='kafka' and t['health']=='up'))
" 2>/dev/null)"
if [[ "${BROKER_COUNT}" -eq 3 ]] 2>/dev/null; then
  pass "3/3 Kafka brokers up"
else
  warn "${BROKER_COUNT:-0}/3 Kafka brokers up (this CRC node cannot currently sustain 3 real brokers - see docs/observability-architecture.md)"
fi

# 16. Alert rules loaded
RULE_COUNT="$(curl -s "http://localhost:${PROM_PF_PORT}/api/v1/rules" 2>/dev/null | python3 -c "
import json,sys
d=json.load(sys.stdin)
mine = [g for g in d['data']['groups'] if g['name'].endswith('.alerts') or g['name'].endswith('.recording')]
print(sum(len(g['rules']) for g in mine))
" 2>/dev/null)"
[[ "${RULE_COUNT}" -ge 1 ]] 2>/dev/null && pass "${RULE_COUNT} of our own alerting/recording rules loaded" || fail "no rules loaded"

echo
echo "=== Control Center availability (existing platform, not this stack) ==="
oc get pod controlcenter-0 -n confluent -o jsonpath='{.status.containerStatuses[?(@.name=="controlcenter")].ready}' 2>/dev/null | grep -q true \
  && pass "Control Center is available" || fail "Control Center is not ready"

echo
if [[ "${FAILURES}" -eq 0 ]]; then
  echo "All critical checks passed."
  exit 0
else
  echo "${FAILURES} critical check(s) failed."
  exit 1
fi
