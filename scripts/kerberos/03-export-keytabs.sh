#!/usr/bin/env bash
# Exports the two service accounts created by 02-create-principals.sh into
# keytab files inside the samba-ad pod (samba-tool domain exportkeytab),
# then copies them out to a local scratch directory for 04-seal-keytabs.sh
# to seal.
#
# exportkeytab (unlike user create) always writes the account's *current*
# key - running this twice for the same principal produces the same
# keytab unless the account's password has changed since.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
OUT_DIR="${REPO_ROOT}/.kerberos-keytabs"
mkdir -p "${OUT_DIR}"

NAMESPACE="auth-services"
REALM="PSYNCOPATE.COM"
SQLSERVER_HOST="${SQLSERVER_HOST:?Set SQLSERVER_HOST to the same value used in 02-create-principals.sh}"
SQLSERVER_PORT="${SQLSERVER_PORT:-1433}"
CONNECT_PRINC="connect/connect.confluent.svc.cluster.local@${REALM}"
SQLSERVER_PRINC="MSSQLSvc/${SQLSERVER_HOST}:${SQLSERVER_PORT}@${REALM}"

SAMBA_POD="$(oc get pod -l app=samba-ad -n "${NAMESPACE}" -o jsonpath='{.items[0].metadata.name}')"

echo "==> Exporting connect.keytab"
oc exec "${SAMBA_POD}" -n "${NAMESPACE}" -- rm -f /tmp/connect.keytab
oc exec "${SAMBA_POD}" -n "${NAMESPACE}" -- samba-tool domain exportkeytab /tmp/connect.keytab --principal="${CONNECT_PRINC}"
oc cp "${NAMESPACE}/${SAMBA_POD}:/tmp/connect.keytab" "${OUT_DIR}/connect.keytab"

echo "==> Exporting mssql.keytab"
oc exec "${SAMBA_POD}" -n "${NAMESPACE}" -- rm -f /tmp/mssql.keytab
oc exec "${SAMBA_POD}" -n "${NAMESPACE}" -- samba-tool domain exportkeytab /tmp/mssql.keytab --principal="${SQLSERVER_PRINC}"
oc cp "${NAMESPACE}/${SAMBA_POD}:/tmp/mssql.keytab" "${OUT_DIR}/mssql.keytab"

echo "==> Removing keytabs from inside the pod"
oc exec "${SAMBA_POD}" -n "${NAMESPACE}" -- rm -f /tmp/connect.keytab /tmp/mssql.keytab

echo "==> Keytabs written to ${OUT_DIR}/ (gitignored - never commit raw keytabs)."
echo "    connect.keytab: sealed into git next, via ./scripts/kerberos/04-seal-keytabs.sh"
echo "    mssql.keytab: SQL Server runs outside this cluster - mount ${OUT_DIR}/mssql.keytab"
echo "    directly into its container (e.g. docker cp), it is never sealed into git."
