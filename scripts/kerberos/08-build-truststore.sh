#!/usr/bin/env bash
# Regenerates base/confluent-platform/connect-truststore-configmap.yaml
# from Schema Registry's live TLS CA cert. Only needs re-running if that
# CA rotates (cert-manager renewal) - the JDBC Source Connector's Avro
# serializer needs to trust it or record production fails with "PKIX path
# building failed", separately from (and after) the Kerberos/JDBC auth
# succeeding.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
WORK_DIR="$(mktemp -d)"
trap 'rm -rf "${WORK_DIR}"' EXIT

echo "==> Fetching schemaregistry-tls-secret's CA cert"
oc get secret schemaregistry-tls-secret -n confluent -o jsonpath='{.data.ca\.crt}' | base64 -d > "${WORK_DIR}/sr-ca.crt"

echo "==> Building JKS truststore"
keytool -importcert -noprompt -alias schemaregistry-ca \
  -file "${WORK_DIR}/sr-ca.crt" -keystore "${WORK_DIR}/truststore.jks" -storepass changeit

echo "==> Writing ConfigMap manifest"
B64="$(base64 < "${WORK_DIR}/truststore.jks" | tr -d '\n')"
cat > "${REPO_ROOT}/base/confluent-platform/connect-truststore-configmap.yaml" <<EOF
# JKS truststore containing Schema Registry's CA cert, so Connect's JVM
# trusts its TLS certificate when the JDBC Source Connector's Avro
# serializer calls it. Confirmed live: without this, record production
# fails with "PKIX path building failed" even though the JDBC/Kerberos
# side is completely healthy - unrelated to Kerberos, just a self-signed
# cert the JVM's default truststore doesn't know about.
#
# Regenerate with this script if schemaregistry-tls-secret's CA ever
# rotates (cert-manager renewal) - this binaryData blob is a point-in-time
# snapshot, not auto-refreshing.
apiVersion: v1
kind: ConfigMap
metadata:
  name: connect-truststore
  namespace: confluent
binaryData:
  truststore.jks: ${B64}
EOF

echo "==> base/confluent-platform/connect-truststore-configmap.yaml updated"
echo "    apply directly for immediate effect: oc apply -f base/confluent-platform/connect-truststore-configmap.yaml"
echo "    (trustStorePassword is 'changeit' - not a secret, this store holds only a public CA cert)"
