#!/usr/bin/env bash
# End-to-end health check for the Samba AD/Kerberos/Connect stack. Run
# after 02-05 have all completed. Non-destructive - read-only checks
# throughout. SQL Server itself runs outside this cluster (see
# docs/kerberos-architecture.md) and isn't checked here - validate it
# directly against wherever it's actually running.
set -uo pipefail

pass() { echo "  OK: $1"; }
fail() { echo "  FAIL: $1"; }

echo "=== Pod status ==="
oc get pods -n auth-services
oc get pods -n confluent -l app=connect

echo
echo "=== Argo CD Application health ==="
oc get application auth-services -n argocd -o wide 2>/dev/null

echo
echo "=== Samba AD: domain provisioned and service accounts present? ==="
SAMBA_ADMIN_PASSWORD="$(oc get secret samba-ad-credentials -n auth-services -o jsonpath='{.data.SAMBA_ADMIN_PASSWORD}' 2>/dev/null | base64 -d)"
SAMBA_POD="$(oc get pod -l app=samba-ad -n auth-services -o jsonpath='{.items[0].metadata.name}' 2>/dev/null)"
if [[ -n "${SAMBA_POD}" ]]; then
  if oc exec "${SAMBA_POD}" -n auth-services -- test -f /var/lib/samba/private/sam.ldb 2>/dev/null; then
    pass "sam.ldb present (domain provisioned)"
  else
    fail "no sam.ldb - domain never provisioned, check pod logs"
  fi
  echo "  Service accounts:"
  oc exec "${SAMBA_POD}" -n auth-services -- samba-tool user list -U "administrator%${SAMBA_ADMIN_PASSWORD}" 2>/dev/null | sed 's/^/    /'
else
  fail "no samba-ad pod found"
fi

echo
echo "=== Connect: keytab mounted and non-empty? ==="
CONNECT_POD="$(oc get pod -l app=connect -n confluent -o jsonpath='{.items[0].metadata.name}' 2>/dev/null)"
if [[ -n "${CONNECT_POD}" ]]; then
  SIZE="$(oc exec "${CONNECT_POD}" -n confluent -- stat -c%s /mnt/secrets/connect-keytab/connect.keytab 2>/dev/null || echo 0)"
  if [[ "${SIZE}" -gt 2 ]]; then
    pass "connect.keytab is ${SIZE} bytes (real keytab, not the 2-byte placeholder)"
  else
    fail "connect.keytab is still the empty placeholder - run scripts/kerberos/03-04"
  fi
else
  fail "no connect pod found"
fi

echo
echo "=== SQL Server: TDS port reachable from Connect? ==="
if [[ -n "${CONNECT_POD:-}" && -n "${SQLSERVER_HOST:-}" ]]; then
  SQLSERVER_PORT="${SQLSERVER_PORT:-1433}"
  if oc exec "${CONNECT_POD}" -n confluent -- bash -c "exec 3<>/dev/tcp/${SQLSERVER_HOST}/${SQLSERVER_PORT}" 2>/dev/null; then
    pass "TCP ${SQLSERVER_PORT} reachable from connect pod"
  else
    fail "cannot reach ${SQLSERVER_HOST}:${SQLSERVER_PORT} from connect - check bootstrap/network-policies.yaml's connect-external-egress"
  fi
else
  echo "  SKIPPED: set SQLSERVER_HOST (and optionally SQLSERVER_PORT) to check this"
fi

echo
echo "=== Connect REST API: connector status ==="
CONNECT_URL="${CONNECT_URL:-https://connect.apps-crc.testing}"
if curl -sk "${CONNECT_URL}/connectors/sqlserver-claims-source/status" 2>/dev/null | grep -q '"state"'; then
  curl -sk "${CONNECT_URL}/connectors/sqlserver-claims-source/status" | python3 -m json.tool
else
  fail "connector not registered yet - run scripts/kerberos/05-deploy-connector.sh"
fi
