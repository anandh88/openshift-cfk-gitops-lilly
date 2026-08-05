#!/usr/bin/env bash
# Runs a full health check across the platform: CFK CRs, pods, certs,
# routes, PVCs, Argo CD apps, a live Kafka topic round-trip, and TLS
# certificate validity. Intended to be run after bootstrap.sh and any
# time you need to sanity-check the cluster.
set -euo pipefail

OCP_DOMAIN="apps-crc.testing"

echo "==> Confluent Platform custom resources"
oc get kafka,kraftcontroller,schemaregistry,connect,controlcenter -n confluent -o wide

echo "==> Pods in confluent namespace (expect all Running)"
oc get pods -n confluent -o wide

echo "==> Pods in flink-jobs namespace"
oc get pods -n flink-jobs -o wide

echo "==> Certificates in confluent namespace (expect all READY=True)"
oc get certificates -n confluent

echo "==> Routes in confluent namespace"
oc get routes -n confluent

echo "==> PVCs in confluent namespace (expect all Bound)"
oc get pvc -n confluent

echo "==> Argo CD applications"
argocd app list

echo "==> Kafka topic create/list smoke test"
KAFKA_POD=$(oc get pods -n confluent -l app=kafka -o jsonpath='{.items[0].metadata.name}')
oc exec -n confluent "${KAFKA_POD}" -- kafka-topics \
  --bootstrap-server kafka:9092 \
  --command-config /mnt/sslcerts/client.properties \
  --create --if-not-exists --topic validate-smoke-test --partitions 1 --replication-factor 1
oc exec -n confluent "${KAFKA_POD}" -- kafka-topics \
  --bootstrap-server kafka:9092 \
  --command-config /mnt/sslcerts/client.properties \
  --list

echo "==> TLS certificate validity (kafka-tls-secret via openssl)"
echo | openssl s_client -connect "kafka-bootstrap.${OCP_DOMAIN}:443" -servername "kafka-bootstrap.${OCP_DOMAIN}" 2>/dev/null \
  | openssl x509 -noout -dates -subject

echo "==> Access URLs"
cat <<EOF
  Argo CD UI:        https://argocd-server.${OCP_DOMAIN}
  Control Center:    https://controlcenter.${OCP_DOMAIN}
  Schema Registry:   https://schemaregistry.${OCP_DOMAIN}
  Kafka Connect:     https://connect.${OCP_DOMAIN}
  Kafka REST Proxy:  https://kafkarestproxy.${OCP_DOMAIN}
EOF

echo "==> Validation complete"
