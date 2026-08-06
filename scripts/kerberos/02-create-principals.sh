#!/usr/bin/env bash
# Creates the two AD service accounts this feature needs, via samba-tool
# (runs locally on the samba-ad pod - no remote auth needed for user
# create/spn add when run directly against the local sam.ldb).
#
#   connect-svc, SPN connect/connect.confluent.svc.cluster.local
#     - Kafka Connect's client principal (JDBC Source Connector's Kerberos
#       identity when authenticating to SQL Server).
#   mssql-svc, SPN MSSQLSvc/<SQLSERVER_HOST>:<SQLSERVER_PORT>
#     - SQL Server's own service principal (SPN format confirmed against
#       Microsoft's AD-authentication tutorial: MSSQLSvc/<fqdn>:<port>).
#       SQL Server runs outside this cluster (see
#       docs/kerberos-architecture.md - mssql/server is amd64-only and
#       this cluster's node is arm64), so its address is environment-
#       specific - override SQLSERVER_HOST/SQLSERVER_PORT rather than
#       assuming an in-cluster Service DNS name.
#
# Safe to re-run: samba-tool user create on an existing user fails ("User
# ... already exists") rather than resetting its password; spn add on an
# already-attached SPN likewise fails without side effects.
set -uo pipefail

NAMESPACE="auth-services"
REALM="PSYNCOPATE.COM"
SQLSERVER_HOST="${SQLSERVER_HOST:?Set SQLSERVER_HOST to the address Connect will use to reach SQL Server, e.g. a host.crc.testing tunnel address or real hostname}"
SQLSERVER_PORT="${SQLSERVER_PORT:-1433}"
ADMIN_PASSWORD="$(oc get secret samba-ad-credentials -n "${NAMESPACE}" -o jsonpath='{.data.SAMBA_ADMIN_PASSWORD}' | base64 -d)"

SAMBA_POD="$(oc get pod -l app=samba-ad -n "${NAMESPACE}" -o jsonpath='{.items[0].metadata.name}')"

echo "==> Creating connect-svc (SPN connect/connect.confluent.svc.cluster.local)"
oc exec "${SAMBA_POD}" -n "${NAMESPACE}" -- samba-tool user create connect-svc "ConnectSvc@Psyncopate2024!" -U "administrator%${ADMIN_PASSWORD}"
oc exec "${SAMBA_POD}" -n "${NAMESPACE}" -- samba-tool spn add "connect/connect.confluent.svc.cluster.local" connect-svc -U "administrator%${ADMIN_PASSWORD}"

echo "==> Creating mssql-svc (SPN MSSQLSvc/${SQLSERVER_HOST}:${SQLSERVER_PORT})"
oc exec "${SAMBA_POD}" -n "${NAMESPACE}" -- samba-tool user create mssql-svc "MssqlSvc@Psyncopate2024!" -U "administrator%${ADMIN_PASSWORD}"
oc exec "${SAMBA_POD}" -n "${NAMESPACE}" -- samba-tool spn add "MSSQLSvc/${SQLSERVER_HOST}:${SQLSERVER_PORT}" mssql-svc -U "administrator%${ADMIN_PASSWORD}"

echo "==> Listing SPNs to confirm"
oc exec "${SAMBA_POD}" -n "${NAMESPACE}" -- samba-tool spn list connect-svc -U "administrator%${ADMIN_PASSWORD}"
oc exec "${SAMBA_POD}" -n "${NAMESPACE}" -- samba-tool spn list mssql-svc -U "administrator%${ADMIN_PASSWORD}"

echo "==> Service accounts created. Next: ./scripts/kerberos/03-export-keytabs.sh"
