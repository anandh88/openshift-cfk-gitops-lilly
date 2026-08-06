#!/usr/bin/env bash
# Seals the real keytabs exported by 03-export-keytabs.sh into the actual
# SealedSecret manifests, overwriting the empty-keytab placeholders
# committed by this feature (base/confluent-platform/secrets/
# connect-keytab-sealed.yaml and base/sqlserver/sqlserver-keytab-sealed.yaml).
# Same kubeseal/public-cert pattern as scripts/seal-secrets.sh.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "${REPO_ROOT}"

CERT_PATH="/tmp/sealed-secrets-public-cert.pem"
KEYTAB_DIR="${REPO_ROOT}/.kerberos-keytabs"

if [[ ! -f "${CERT_PATH}" ]]; then
  echo "ERROR: ${CERT_PATH} not found. Run scripts/bootstrap.sh first, or fetch it manually with:"
  echo "  kubeseal --fetch-cert --controller-name=sealed-secrets --controller-namespace=kube-system > ${CERT_PATH}"
  exit 1
fi

if [[ ! -f "${KEYTAB_DIR}/connect.keytab" || ! -f "${KEYTAB_DIR}/mssql.keytab" ]]; then
  echo "ERROR: ${KEYTAB_DIR}/{connect,mssql}.keytab not found. Run scripts/kerberos/03-export-keytabs.sh first."
  exit 1
fi

echo "==> Sealing connect-keytab (confluent namespace)"
CONNECT_KEYTAB_B64="$(base64 < "${KEYTAB_DIR}/connect.keytab" | tr -d '\n')"
kubeseal --cert "${CERT_PATH}" --format yaml \
  <<EOF > base/confluent-platform/secrets/connect-keytab-sealed.yaml
apiVersion: v1
kind: Secret
metadata:
  name: connect-keytab
  namespace: confluent
type: Opaque
data:
  connect.keytab: ${CONNECT_KEYTAB_B64}
EOF
echo "    -> base/confluent-platform/secrets/connect-keytab-sealed.yaml updated"

echo "==> Sealing sqlserver-keytab (sqlserver namespace)"
MSSQL_KEYTAB_B64="$(base64 < "${KEYTAB_DIR}/mssql.keytab" | tr -d '\n')"
kubeseal --cert "${CERT_PATH}" --format yaml \
  <<EOF > base/sqlserver/sqlserver-keytab-sealed.yaml
apiVersion: v1
kind: Secret
metadata:
  name: sqlserver-keytab
  namespace: sqlserver
type: Opaque
data:
  mssql.keytab: ${MSSQL_KEYTAB_B64}
EOF
echo "    -> base/sqlserver/sqlserver-keytab-sealed.yaml updated"

echo "==> Removing local plaintext keytabs (they're sealed in git now)"
rm -f "${KEYTAB_DIR}/connect.keytab" "${KEYTAB_DIR}/mssql.keytab"

echo "==> Done. Review the diffs, then git add/commit/push so Argo CD picks up the real keytabs."
