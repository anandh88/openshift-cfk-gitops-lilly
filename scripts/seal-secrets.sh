#!/usr/bin/env bash
# Generates real SealedSecret manifests for every credential the platform
# needs, using the sealed-secrets controller's public cert fetched by
# scripts/bootstrap.sh. Overwrites the placeholder files under
# base/confluent-platform/secrets/ and base/flink-jobs/.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

CERT_PATH="/tmp/sealed-secrets-public-cert.pem"

# The public cert is required before any secret can be sealed offline.
if [[ ! -f "${CERT_PATH}" ]]; then
  echo "ERROR: ${CERT_PATH} not found. Run scripts/bootstrap.sh first, or fetch it manually with:"
  echo "  kubeseal --fetch-cert --controller-name=sealed-secrets --controller-namespace=kube-system > ${CERT_PATH}"
  exit 1
fi

echo "==> Using sealed-secrets public cert at ${CERT_PATH}"

# --- kafka-internal-sasl (confluent) -----------------------------------
echo "==> Sealing kafka-internal-sasl"
kubeseal --cert "${CERT_PATH}" --format yaml \
  <<EOF > base/confluent-platform/secrets/kafka-sasl-sealed.yaml
apiVersion: v1
kind: Secret
metadata:
  name: kafka-internal-sasl
  namespace: confluent
type: Opaque
stringData:
  plain.txt: |
    username=kafka-admin
    password=KafkaAdmin@Local2024!
  plain-users.json: |
    {
      "kafka-admin": "KafkaAdmin@Local2024!",
      "flink-user": "FlinkUser@Local2024!",
      "connect-user": "ConnectUser@Local2024!",
      "sr-user": "SchemaRegUser@Local2024!"
    }
  plain-interbroker.txt: |
    username=kafka-admin
    password=KafkaAdmin@Local2024!
EOF
echo "    -> base/confluent-platform/secrets/kafka-sasl-sealed.yaml created"

# --- kafka-external-sasl (confluent) -----------------------------------
echo "==> Sealing kafka-external-sasl"
kubeseal --cert "${CERT_PATH}" --format yaml \
  <<EOF > base/confluent-platform/secrets/kafka-external-sasl-sealed.yaml
apiVersion: v1
kind: Secret
metadata:
  name: kafka-external-sasl
  namespace: confluent
type: Opaque
stringData:
  plain.txt: |
    username=external-admin
    password=ExternalAdmin@Local2024!
  plain-users.json: |
    {
      "external-admin": "ExternalAdmin@Local2024!"
    }
EOF
echo "    -> base/confluent-platform/secrets/kafka-external-sasl-sealed.yaml created"

# --- c3-credentials (confluent) -----------------------------------------
echo "==> Sealing c3-credentials"
kubeseal --cert "${CERT_PATH}" --format yaml \
  <<EOF > base/confluent-platform/secrets/c3-credentials-sealed.yaml
apiVersion: v1
kind: Secret
metadata:
  name: c3-credentials
  namespace: confluent
type: Opaque
stringData:
  basic.txt: |
    username=admin
    password=C3Admin@Local2024!
EOF
echo "    -> base/confluent-platform/secrets/c3-credentials-sealed.yaml created"

# --- flink-kafka-sasl (flink-jobs) --------------------------------------
echo "==> Sealing flink-kafka-sasl"
kubeseal --cert "${CERT_PATH}" --format yaml \
  <<EOF > base/flink-jobs/flink-kafka-sasl-sealed.yaml
apiVersion: v1
kind: Secret
metadata:
  name: flink-kafka-sasl
  namespace: flink-jobs
type: Opaque
stringData:
  plain.txt: |
    username=flink-user
    password=FlinkUser@Local2024!
EOF
echo "    -> base/flink-jobs/flink-kafka-sasl-sealed.yaml created"

# --- connect-keytab (confluent) ------------------------------------------
# NOT regenerated here - it holds a binary Kerberos keytab, not a password,
# and this script only ever seals plaintext stringData. Its real value
# comes from scripts/kerberos/04-seal-keytabs.sh after scripts/kerberos/
# 01-03 have run against a live KDC. Re-running this script will NOT touch
# base/confluent-platform/secrets/connect-keytab-sealed.yaml - leave the
# real keytab (or the empty-keytab placeholder) in place. SQL Server itself
# runs outside this cluster (see docs/kerberos-architecture.md), so its
# credentials/keytab are no longer sealed here at all.
echo "==> Skipping connect-keytab - see scripts/kerberos/04-seal-keytabs.sh"

# --- grafana-admin (monitoring) -----------------------------------------
echo "==> Sealing grafana-admin"
kubeseal --cert "${CERT_PATH}" --format yaml \
  <<EOF > base/observability/secrets/grafana-admin-sealed.yaml
apiVersion: v1
kind: Secret
metadata:
  name: grafana-admin
  namespace: monitoring
type: Opaque
stringData:
  admin-user: admin
  admin-password: GrafanaAdmin@Local2024!
EOF
echo "    -> base/observability/secrets/grafana-admin-sealed.yaml created"

echo "==> All secrets sealed. Review the diffs, then git add/commit/push."
