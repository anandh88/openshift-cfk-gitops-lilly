#!/usr/bin/env bash
# Lists what Prometheus is actually receiving for a given component group -
# use this BEFORE writing any new dashboard panel or alert rule, per
# docs/prometheus-metrics-guide.md's "discover, don't guess" rule.
#
# Usage:
#   ./scripts/list-platform-metrics.sh kafka
#   ./scripts/list-platform-metrics.sh connect
#   ./scripts/list-platform-metrics.sh flink
#   ./scripts/list-platform-metrics.sh all
set -euo pipefail

GROUP="${1:-all}"
PF_PORT="${PF_PORT:-19210}"

cleanup() { jobs -p | xargs -r kill 2>/dev/null; }
trap cleanup EXIT

oc port-forward -n monitoring svc/observability-prometheus "${PF_PORT}:9090" >/tmp/list-metrics-pf.log 2>&1 &
sleep 4

ALL_NAMES="$(curl -s "http://localhost:${PF_PORT}/api/v1/label/__name__/values")"

case "${GROUP}" in
  kafka)
    PATTERN='^kafka_(server|controller|network|log)_'
    ;;
  connect)
    PATTERN='^kafka_connect_'
    ;;
  schemaregistry)
    PATTERN='^(io_confluent_kafka_schemaregistry|schemaregistry_confluent)_'
    ;;
  restproxy)
    PATTERN='^kafkarestproxy_confluent_'
    ;;
  jvm)
    PATTERN='^(java_lang|jvm)_'
    ;;
  infra)
    PATTERN='^(kube_|container_|kubelet_)'
    ;;
  flink)
    echo "No Flink metrics exist yet on this cluster - the Flink Kubernetes"
    echo "Operator only exposes a plain-text health check on port 8085, not"
    echo "Prometheus format, and no FlinkDeployment/job is currently running."
    echo "See docs/observability-architecture.md for what's needed first."
    exit 0
    ;;
  all)
    PATTERN='.'
    ;;
  *)
    echo "Unknown group: ${GROUP}"
    echo "Valid groups: kafka, connect, schemaregistry, restproxy, jvm, infra, flink, all"
    exit 1
    ;;
esac

echo "${ALL_NAMES}" | python3 -c "
import json, sys, re
d = json.load(sys.stdin)
pattern = re.compile('${PATTERN}')
names = sorted(n for n in d['data'] if pattern.search(n))
print(f'{len(names)} metric(s) matching group \"${GROUP}\":')
for n in names:
    print(' ', n)
"
