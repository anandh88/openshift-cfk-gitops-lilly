#!/usr/bin/env bash
# End-to-end health check for the LDAP/Kerberos/SQL Server stack. Run after
# 01-05 have all completed. Non-destructive - read-only checks throughout.
set -uo pipefail

pass() { echo "  OK: $1"; }
fail() { echo "  FAIL: $1"; }

echo "=== Pod status ==="
oc get pods -n auth-services
oc get pods -n sqlserver
oc get pods -n confluent -l app=connect

echo
echo "=== Argo CD Application health ==="
oc get application auth-services kerberos-kdc sqlserver -n argocd -o wide 2>/dev/null

echo
echo "=== LDAP: kerberos schema loaded? ==="
LDAP_ADMIN_PASSWORD="$(oc get secret ldap-credentials -n auth-services -o jsonpath='{.data.LDAP_ADMIN_PASSWORD}' 2>/dev/null | base64 -d)"
LDAP_POD="$(oc get pod -l app=ldap -n auth-services -o jsonpath='{.items[0].metadata.name}' 2>/dev/null)"
if [[ -n "${LDAP_POD}" ]]; then
  if oc exec "${LDAP_POD}" -n auth-services -- ldapsearch -x -D "cn=admin,dc=psyncopate,dc=com" -w "${LDAP_ADMIN_PASSWORD}" \
      -b "cn=schema,cn=config" -s one dn 2>/dev/null | grep -qi kerberos; then
    pass "kerberos schema present under cn=schema,cn=config"
  else
    fail "kerberos schema NOT found - check base/ldap/ldap-seed-configmap.yaml bootstrap logs"
  fi
else
  fail "no ldap pod found"
fi

echo
echo "=== KDC: realm initialized? ==="
KDC_POD="$(oc get pod -l app=kdc -n auth-services -o jsonpath='{.items[0].metadata.name}' 2>/dev/null)"
if [[ -n "${KDC_POD}" ]]; then
  if oc exec "${KDC_POD}" -n auth-services -- test -f /var/lib/krb5kdc/.k5.PSYNCOPATE.COM 2>/dev/null; then
    pass "KDC stash file present (realm initialized)"
  else
    fail "no stash file - run scripts/kerberos/01-init-kdc.sh"
  fi
  echo "  Principals:"
  oc exec "${KDC_POD}" -n auth-services -- kadmin.local -q "listprincs" 2>/dev/null | sed 's/^/    /'
else
  fail "no kdc pod found"
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
if [[ -n "${CONNECT_POD:-}" ]]; then
  if oc exec "${CONNECT_POD}" -n confluent -- bash -c "exec 3<>/dev/tcp/sqlserver.sqlserver.svc.cluster.local/1433" 2>/dev/null; then
    pass "TCP 1433 reachable from connect pod"
  else
    fail "cannot reach sqlserver:1433 from connect - check bootstrap/auth-services-network-policies.yaml"
  fi
fi

echo
echo "=== Connect REST API: connector status ==="
CONNECT_URL="${CONNECT_URL:-https://connect.apps-crc.testing}"
if curl -sk "${CONNECT_URL}/connectors/sqlserver-claims-source/status" 2>/dev/null | grep -q '"state"'; then
  curl -sk "${CONNECT_URL}/connectors/sqlserver-claims-source/status" | python3 -m json.tool
else
  fail "connector not registered yet - run scripts/kerberos/05-deploy-connector.sh"
fi
