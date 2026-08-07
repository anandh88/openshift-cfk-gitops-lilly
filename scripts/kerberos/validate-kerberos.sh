#!/usr/bin/env bash
# End-to-end health check for the Samba AD (Docker Desktop)/Kerberos/
# Connect/SQL Server stack. Run after scripts/kerberos/setup-kerberos.sh has completed.
# Non-destructive - read-only checks throughout.
set -uo pipefail

pass() { echo "  OK: $1"; }
fail() { echo "  FAIL: $1"; }

ADMIN_PASSWORD="${SAMBA_ADMIN_PASSWORD:-SambaAdmin@Psyncopate2024!}"

echo "=== Docker containers ==="
docker ps --filter name=sambaad --filter name=sqltest2 --format '{{.Names}}: {{.Status}}'

echo
echo "=== Samba AD DC: domain provisioned and accounts present? ==="
if docker exec sambaad test -f /var/lib/samba/private/sam.ldb 2>/dev/null; then
  pass "sam.ldb present (domain provisioned)"
  docker exec sambaad samba-tool user list -U "administrator%${ADMIN_PASSWORD}" 2>/dev/null | grep -E "connect-svc|mssql-svc" | sed 's/^/    /'
else
  fail "no sam.ldb - run scripts/kerberos/setup-kerberos.sh"
fi

echo
echo "=== SQL Server: sssd resolving AD identities? ==="
if docker exec sqltest2 getent passwd connect-svc@psyncopate.com >/dev/null 2>&1; then
  pass "sssd resolves connect-svc@psyncopate.com - SQL Server trusts the domain"
else
  fail "sssd cannot resolve AD identities - run scripts/kerberos/setup-kerberos.sh (note: docker restart sqltest2 wipes this, re-run after any restart)"
fi

echo
echo "=== Connect tunnel: node reachable on the Kerberos port? ==="
if pgrep -f "ssh.*-R 0.0.0.0:18088" >/dev/null 2>&1; then
  pass "reverse tunnel process running"
else
  fail "no tunnel process found - run scripts/kerberos/setup-kerberos.sh"
fi

echo
echo "=== Pod status ==="
oc get pods -n confluent -l app=connect

echo
echo "=== Connect: keytab mounted and non-empty? ==="
CONNECT_POD="$(oc get pod -l app=connect -n confluent -o jsonpath='{.items[0].metadata.name}' 2>/dev/null)"
if [[ -n "${CONNECT_POD}" ]]; then
  SIZE="$(oc exec "${CONNECT_POD}" -n confluent -c connect -- wc -c /mnt/secrets/connect-keytab/connect.keytab 2>/dev/null | awk '{print $1}')"
  if [[ -n "${SIZE}" && "${SIZE}" -gt 2 ]]; then
    pass "connect.keytab is ${SIZE} bytes (real keytab, not the 2-byte placeholder)"
  else
    fail "connect.keytab is still the empty placeholder - run scripts/kerberos/setup-kerberos.sh"
  fi
else
  fail "no connect pod found"
fi

echo
echo "=== Connect REST API: connector status ==="
CONNECT_URL="${CONNECT_URL:-https://connect.apps-crc.testing}"
STATUS_JSON="$(curl -sk "${CONNECT_URL}/connectors/sqlserver-claims-source/status" 2>/dev/null)"
if echo "${STATUS_JSON}" | grep -q '"state":"RUNNING"'; then
  pass "connector and task RUNNING"
else
  fail "connector not RUNNING - run scripts/kerberos/setup-kerberos.sh"
fi
echo "${STATUS_JSON}" | python3 -m json.tool 2>/dev/null || echo "${STATUS_JSON}"
