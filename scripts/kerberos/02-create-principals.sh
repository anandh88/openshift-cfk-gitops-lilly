#!/usr/bin/env bash
# Creates the two service principals this feature needs, via kadmin.local
# (runs locally on the KDC pod, authenticating to the LDAP backend using
# the stash file 01-init-kdc.sh created - no password prompt needed).
#
#   connect/connect.confluent.svc.cluster.local@PSYNCOPATE.COM
#     - Kafka Connect's client principal (JDBC Source Connector's Kerberos
#       identity when authenticating to SQL Server).
#   MSSQLSvc/sqlserver.sqlserver.svc.cluster.local:1433@PSYNCOPATE.COM
#     - SQL Server's own service principal (SPN format confirmed against
#       Microsoft's AD-authentication tutorial: MSSQLSvc/<fqdn>:<port>).
#
# -randkey: no human ever needs to know these passwords - both principals
# only ever get used via keytab (see 03-export-keytabs.sh), matching the
# same pattern as this repo's other machine-to-machine SASL credentials.
#
# Safe to re-run: kadmin.local addprinc on an existing principal fails
# ("already exists") rather than resetting its key.
set -euo pipefail

NAMESPACE="auth-services"
REALM="PSYNCOPATE.COM"
CONNECT_PRINC="connect/connect.confluent.svc.cluster.local@${REALM}"
SQLSERVER_PRINC="MSSQLSvc/sqlserver.sqlserver.svc.cluster.local:1433@${REALM}"

KDC_POD="$(oc get pod -l app=kdc -n "${NAMESPACE}" -o jsonpath='{.items[0].metadata.name}')"

echo "==> Creating ${CONNECT_PRINC}"
oc exec "${KDC_POD}" -n "${NAMESPACE}" -- kadmin.local -q "addprinc -randkey ${CONNECT_PRINC}"

echo "==> Creating ${SQLSERVER_PRINC}"
oc exec "${KDC_POD}" -n "${NAMESPACE}" -- kadmin.local -q "addprinc -randkey ${SQLSERVER_PRINC}"

echo "==> Listing principals to confirm"
oc exec "${KDC_POD}" -n "${NAMESPACE}" -- kadmin.local -q "listprincs"

echo "==> Principals created. Next: ./scripts/kerberos/03-export-keytabs.sh"
