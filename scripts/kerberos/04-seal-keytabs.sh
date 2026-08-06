#!/usr/bin/env bash
# Seals the real connect.keytab exported by 03-export-keytabs.sh into the
# actual SealedSecret manifest, overwriting the empty-keytab placeholder
# committed by this feature (base/confluent-platform/secrets/
# connect-keytab-sealed.yaml). Same kubeseal/public-cert pattern as
# scripts/seal-secrets.sh.
#
# mssql.keytab is NOT sealed here - SQL Server runs outside this cluster
# (see docs/kerberos-architecture.md), so its keytab is mounted directly
# into its container rather than going through a Kubernetes Secret.
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

if [[ ! -f "${KEYTAB_DIR}/connect.keytab" ]]; then
  echo "ERROR: ${KEYTAB_DIR}/connect.keytab not found. Run scripts/kerberos/03-export-keytabs.sh first."
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

echo "==> Removing local plaintext connect.keytab (it's sealed in git now)"
rm -f "${KEYTAB_DIR}/connect.keytab"

echo "==> Done. Review the diff, then git add/commit/push so Argo CD picks up the real keytab."
echo "    mssql.keytab (if present in ${KEYTAB_DIR}/) is left as-is - mount it directly"
echo "    into your SQL Server container, it is never sealed into git."