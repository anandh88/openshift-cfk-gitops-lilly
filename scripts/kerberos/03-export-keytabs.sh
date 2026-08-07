#!/usr/bin/env bash
# Exports the two accounts created by 02-create-principals.sh into keytab
# files on the Samba AD DC container, then copies them out to a local
# scratch directory for 04-seal-keytabs.sh (connect.keytab) and
# 06-join-sqlserver-domain.sh (mssql.keytab) to consume.
#
# connect.keytab is exported for the ACCOUNT principal (connect-svc@
# PSYNCOPATE.COM) - see 02-create-principals.sh's header comment for why
# this must be the account identity, not the SPN string.
#
# exportkeytab always writes the account's *current* key - running this
# twice for the same principal produces the same keytab unless the
# account's password has changed since.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
OUT_DIR="${REPO_ROOT}/.kerberos-keytabs"
mkdir -p "${OUT_DIR}"

CONTAINER="sambaad"
REALM="PSYNCOPATE.COM"
SQLSERVER_HOST="${SQLSERVER_HOST:?Set SQLSERVER_HOST to the same value used in 02-create-principals.sh}"
SQLSERVER_PORT="${SQLSERVER_PORT:-1433}"

echo "==> Exporting connect.keytab (principal connect-svc@${REALM})"
docker exec "${CONTAINER}" rm -f /tmp/connect.keytab
docker exec "${CONTAINER}" samba-tool domain exportkeytab /tmp/connect.keytab --principal="connect-svc@${REALM}"
docker cp "${CONTAINER}:/tmp/connect.keytab" "${OUT_DIR}/connect.keytab"

echo "==> Exporting mssql.keytab (principal MSSQLSvc/${SQLSERVER_HOST}:${SQLSERVER_PORT}@${REALM})"
docker exec "${CONTAINER}" rm -f /tmp/mssql.keytab
docker exec "${CONTAINER}" samba-tool domain exportkeytab /tmp/mssql.keytab --principal="MSSQLSvc/${SQLSERVER_HOST}:${SQLSERVER_PORT}@${REALM}"
docker cp "${CONTAINER}:/tmp/mssql.keytab" "${OUT_DIR}/mssql.keytab"

echo "==> Removing keytabs from inside the container"
docker exec "${CONTAINER}" rm -f /tmp/connect.keytab /tmp/mssql.keytab

echo "==> Keytabs written to ${OUT_DIR}/ (gitignored - never commit raw keytabs)."
echo "    connect.keytab: sealed into git next, via ./scripts/kerberos/04-seal-keytabs.sh"
echo "    mssql.keytab: mounted directly into SQL Server next, via"
echo "    ./scripts/kerberos/06-join-sqlserver-domain.sh - never sealed into git."
