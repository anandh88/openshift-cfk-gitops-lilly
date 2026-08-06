#!/usr/bin/env bash
# One-time KDC initialization: creates the PSYNCOPATE.COM realm in the
# LDAP-backed principal store (kdb5_ldap_util create), stashes the KDC's
# own LDAP bind password so krb5kdc/kadmind can authenticate to LDAP
# without a password prompt, then restarts the kdc Deployment so its
# startup script (which waits/sleeps until this has run - see
# base/kerberos/kdc-deployment.yaml) proceeds to start krb5kdc + kadmind.
#
# Safe to re-run: `kdb5_ldap_util create` on an already-initialized realm
# fails loudly ("already exists") rather than silently corrupting state;
# if you see that, initialization already happened - skip to
# 02-create-principals.sh.
set -euo pipefail

NAMESPACE="auth-services"
REALM="PSYNCOPATE.COM"
BASE_DN="dc=psyncopate,dc=com"
BIND_DN="cn=admin,${BASE_DN}"
LDAP_URI="ldap://ldap.auth-services.svc.cluster.local"

echo "==> Fetching LDAP admin password from the ldap-credentials secret"
LDAP_ADMIN_PASSWORD="$(oc get secret ldap-credentials -n "${NAMESPACE}" -o jsonpath='{.data.LDAP_ADMIN_PASSWORD}' | base64 -d)"

echo "==> Waiting for kdc and ldap pods to be ready"
oc wait --for=condition=Ready pod -l app=ldap -n "${NAMESPACE}" --timeout=120s
oc wait --for=condition=Ready pod -l app=kdc -n "${NAMESPACE}" --timeout=180s || true
KDC_POD="$(oc get pod -l app=kdc -n "${NAMESPACE}" -o jsonpath='{.items[0].metadata.name}')"

echo "==> Creating realm ${REALM} in LDAP (kdb5_ldap_util create)"
# Prompts, in order: LDAP bind password, then twice for the new KDC master
# database password (a separate secret from the LDAP admin password -
# this one only ever lives inside the stash file created by -s below,
# never in a Kubernetes Secret).
MASTER_PW="$(oc get secret ldap-credentials -n "${NAMESPACE}" -o jsonpath='{.data.LDAP_KDC_BIND_PASSWORD}' | base64 -d)"
oc exec -i "${KDC_POD}" -n "${NAMESPACE}" -- bash -c "
  kdb5_ldap_util -D '${BIND_DN}' -H '${LDAP_URI}' create \
    -subtrees 'ou=kerberos,${BASE_DN}' -r '${REALM}' -s
" <<EOF
${LDAP_ADMIN_PASSWORD}
${MASTER_PW}
${MASTER_PW}
EOF

echo "==> Stashing the KDC's own LDAP bind password (for krb5kdc/kadmind's runtime binds)"
oc exec -i "${KDC_POD}" -n "${NAMESPACE}" -- bash -c "
  kdb5_ldap_util -D '${BIND_DN}' -H '${LDAP_URI}' stashsrvpw \
    -f /var/lib/krb5kdc/ldap_service_password '${BIND_DN}'
" <<EOF
${LDAP_ADMIN_PASSWORD}
${LDAP_ADMIN_PASSWORD}
EOF

echo "==> Restarting kdc Deployment so krb5kdc + kadmind start"
oc rollout restart deployment/kdc -n "${NAMESPACE}"
oc rollout status deployment/kdc -n "${NAMESPACE}" --timeout=180s

echo "==> KDC initialized. Next: ./scripts/kerberos/02-create-principals.sh"
