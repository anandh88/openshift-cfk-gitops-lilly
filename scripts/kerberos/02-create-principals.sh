#!/usr/bin/env bash
# Creates the two AD accounts this feature needs, on the Docker Desktop AD
# DC (see 01-setup-docker-ad.sh).
#
#   connect-svc  - Kafka Connect's own Kerberos identity. Its keytab must
#     be exported for the ACCOUNT principal "connect-svc@PSYNCOPATE.COM",
#     not a service-style "connect/host@REALM" string - confirmed live
#     that AD rejects the latter with "Client not found in Kerberos
#     database" when used for kinit/AS-REQ. Unlike MIT Kerberos (where any
#     string can be a client principal), AD only lets an account's own
#     identity authenticate; SPNs are lookup aliases for services, not
#     separate client identities.
#   mssql-svc  - SQL Server's own service identity, with an SPN attached
#     matching exactly the host:port Connect uses to reach it
#     (MSSQLSvc/<SQLSERVER_HOST>:<SQLSERVER_PORT> - SPN format confirmed
#     against Microsoft's AD-authentication tutorial).
#
# Safe to re-run: samba-tool user create on an existing user fails ("User
# ... already exists") rather than resetting its password; spn add on an
# already-attached SPN likewise fails without side effects.
set -uo pipefail

CONTAINER="sambaad"
ADMIN_PASSWORD="${SAMBA_ADMIN_PASSWORD:-SambaAdmin@Psyncopate2024!}"
SQLSERVER_HOST="${SQLSERVER_HOST:?Set SQLSERVER_HOST to the address Connect will use to reach SQL Server (see docs/kerberos-architecture.md for the reverse-tunnel address)}"
SQLSERVER_PORT="${SQLSERVER_PORT:-1433}"

echo "==> Creating connect-svc"
docker exec "${CONTAINER}" samba-tool user create connect-svc "ConnectSvc@Psyncopate2024!" -U "administrator%${ADMIN_PASSWORD}"

echo "==> Creating mssql-svc (SPN MSSQLSvc/${SQLSERVER_HOST}:${SQLSERVER_PORT})"
docker exec "${CONTAINER}" samba-tool user create mssql-svc "MssqlSvc@Psyncopate2024!" -U "administrator%${ADMIN_PASSWORD}"
docker exec "${CONTAINER}" samba-tool spn add "MSSQLSvc/${SQLSERVER_HOST}:${SQLSERVER_PORT}" mssql-svc -U "administrator%${ADMIN_PASSWORD}"

echo "==> Confirming"
docker exec "${CONTAINER}" samba-tool user list -U "administrator%${ADMIN_PASSWORD}" | grep -E "connect-svc|mssql-svc"
docker exec "${CONTAINER}" samba-tool spn list mssql-svc -U "administrator%${ADMIN_PASSWORD}"

echo "==> Accounts created. Next: ./scripts/kerberos/03-export-keytabs.sh"
