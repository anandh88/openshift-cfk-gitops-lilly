#!/usr/bin/env bash
# Exports the two principals created by 02-create-principals.sh into
# keytab files inside the KDC pod (kadmin.local ktadd), then copies them
# out to a local scratch directory for 04-seal-keytabs.sh to seal.
#
# ktadd (unlike addprinc -randkey) generates a NEW random key and writes it
# both into the keytab and into the LDAP-backed principal entry - running
# this twice for the same principal invalidates any previously-exported
# keytab for it (expected kadmin behavior, not specific to this repo).
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
OUT_DIR="${REPO_ROOT}/.kerberos-keytabs"
mkdir -p "${OUT_DIR}"

NAMESPACE="auth-services"
REALM="PSYNCOPATE.COM"
CONNECT_PRINC="connect/connect.confluent.svc.cluster.local@${REALM}"
SQLSERVER_PRINC="MSSQLSvc/sqlserver.sqlserver.svc.cluster.local:1433@${REALM}"

KDC_POD="$(oc get pod -l app=kdc -n "${NAMESPACE}" -o jsonpath='{.items[0].metadata.name}')"

echo "==> Exporting connect.keytab"
oc exec "${KDC_POD}" -n "${NAMESPACE}" -- kadmin.local -q "ktadd -k /tmp/connect.keytab ${CONNECT_PRINC}"
oc cp "${NAMESPACE}/${KDC_POD}:/tmp/connect.keytab" "${OUT_DIR}/connect.keytab"

echo "==> Exporting mssql.keytab"
oc exec "${KDC_POD}" -n "${NAMESPACE}" -- kadmin.local -q "ktadd -k /tmp/mssql.keytab ${SQLSERVER_PRINC}"
oc cp "${NAMESPACE}/${KDC_POD}:/tmp/mssql.keytab" "${OUT_DIR}/mssql.keytab"

echo "==> Removing keytabs from inside the pod (they now only need to exist sealed in git)"
oc exec "${KDC_POD}" -n "${NAMESPACE}" -- rm -f /tmp/connect.keytab /tmp/mssql.keytab

echo "==> Keytabs written to ${OUT_DIR}/ (gitignored - never commit raw keytabs)."
echo "    Next: ./scripts/kerberos/04-seal-keytabs.sh"
