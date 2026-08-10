#!/usr/bin/env bash
# Single end-to-end, idempotent bootstrap for the whole local CFK + SQL
# Server + Kerberos stack. Safe to re-run in full any time (after a Mac
# reboot, `crc stop`/`crc start`, or just to reconcile drift) - every step
# checks current state before acting instead of blindly reapplying.
#
# Composes the existing scripts/bootstrap.sh (cert-manager, sealed-secrets,
# RBAC/NetworkPolicies, Argo CD) and scripts/kerberos/setup-kerberos.sh
# (Samba AD DC, keytabs, SQL Server domain-join, tunnels, connector) with
# the steps this repo doesn't otherwise automate: approving the CFK
# operator's manual InstallPlan, standing up the SQL Server container and
# its own reverse tunnel, and applying the platform overlay directly
# (deterministic - doesn't depend on Argo CD's own reconcile timing, which
# has been unreliable under this CRC VM's resource pressure; Argo CD is
# still handed the app-of-apps at the end for ongoing self-heal).
#
# Usage: ./scripts/bootstrap-all.sh
# Optional env overrides: SA_PASSWORD, CLAIMS_DB, SQLSERVER_CONTAINER,
# SQLSERVER_PORT, SAMBA_ADMIN_PASSWORD (see scripts/kerberos/setup-kerberos.sh
# for the Kerberos-specific ones).
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${REPO_ROOT}"

SQLSERVER_CONTAINER="${SQLSERVER_CONTAINER:-sqltest2}"
SA_PASSWORD="${SA_PASSWORD:-YourPassword123!}"
CLAIMS_DB="${CLAIMS_DB:-claims_db}"
SQLSERVER_PORT="${SQLSERVER_PORT:-14330}"
CRC_SSH=(ssh -i "$HOME/.crc/machines/crc/id_ed25519" -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -p 2222 core@127.0.0.1)

pass() { echo "  OK: $1"; }
fail() { echo "  FAIL: $1"; }
step() { echo; echo "############################################################"; echo "# $1"; echo "############################################################"; }
# The CRC API server has been observed to intermittently reset connections
# under sustained load ("read: connection reset by peer") - retries a
# transient failure a few times before giving up, rather than treating it
# as a hard error on the first blip.
retry() {
  local attempts="$1"; shift
  local i
  for ((i = 1; i <= attempts; i++)); do
    if "$@"; then return 0; fi
    [[ "${i}" -lt "${attempts}" ]] && sleep 10
  done
  return 1
}

step "0. Preflight"
for bin in oc docker crc ssh kubeseal helm; do
  command -v "${bin}" >/dev/null 2>&1 && pass "${bin} found" || fail "${bin} not found on PATH - install it before continuing"
done
WHOAMI="$(retry 5 oc whoami 2>/dev/null || true)"
[[ -n "${WHOAMI}" ]] && pass "logged into OpenShift as ${WHOAMI}" || { fail "not logged in (or API server unreachable after retries) - run: oc login -u kubeadmin -p <password> https://api.crc.testing:6443"; exit 1; }
[[ -f "$HOME/.crc/machines/crc/id_ed25519" ]] && pass "CRC SSH key present" || { fail "CRC SSH key missing - run 'crc start' at least once first"; exit 1; }

step "1. Cluster bootstrap (cert-manager, sealed-secrets, RBAC/NetworkPolicies, Argo CD)"
# scripts/bootstrap.sh is itself idempotent (helm upgrade --install, oc apply),
# so it's safe to always run rather than trying to detect partial state.
# Retried as a whole: a transient API-server blip partway through (observed
# on this CRC VM under load - "stream error ... INTERNAL_ERROR") is cheaper
# to retry from the top than to make every individual step inside it retry-aware.
retry 3 ./scripts/bootstrap.sh

step "2. Seal platform secrets"
if [[ -f /tmp/sealed-secrets-public-cert.pem ]]; then
  ./scripts/seal-secrets.sh
else
  fail "no sealed-secrets public cert at /tmp/sealed-secrets-public-cert.pem - bootstrap.sh should have fetched it; re-run this script"
fi

step "3. CFK operator (OLM subscription + manual InstallPlan approval)"
oc apply -f base/confluent-operator/ >/dev/null
echo "==> Waiting for an InstallPlan to appear"
for i in $(seq 1 30); do
  PLAN_NAME="$(oc get installplan -n confluent-operator -o jsonpath='{.items[0].metadata.name}' 2>/dev/null)"
  [[ -n "${PLAN_NAME}" ]] && break
  sleep 5
done
if [[ -z "${PLAN_NAME}" ]]; then
  fail "no InstallPlan appeared after 150s - check 'oc get subscription -n confluent-operator'"
else
  APPROVED="$(oc get installplan "${PLAN_NAME}" -n confluent-operator -o jsonpath='{.spec.approved}' 2>/dev/null)"
  if [[ "${APPROVED}" == "true" ]]; then
    pass "InstallPlan ${PLAN_NAME} already approved"
  else
    oc patch installplan "${PLAN_NAME}" -n confluent-operator --type merge -p '{"spec":{"approved":true}}' >/dev/null
    pass "InstallPlan ${PLAN_NAME} approved"
  fi
fi
echo "==> Waiting for confluent-operator deployment to become ready"
oc wait deployment/confluent-operator -n confluent-operator --for=condition=Available --timeout=300s 2>&1 || fail "confluent-operator not Available after 300s - check 'oc get pods -n confluent-operator'"

step "4. SQL Server container (Docker Desktop) + claims_db/claims table"
if ! docker inspect "${SQLSERVER_CONTAINER}" >/dev/null 2>&1; then
  echo "==> Creating ${SQLSERVER_CONTAINER}"
  docker run -d --name "${SQLSERVER_CONTAINER}" --restart unless-stopped \
    -e "ACCEPT_EULA=Y" -e "SA_PASSWORD=${SA_PASSWORD}" -p 1433:1433 \
    mcr.microsoft.com/mssql/server:2019-latest >/dev/null
  pass "${SQLSERVER_CONTAINER} created"
elif [[ "$(docker inspect -f '{{.State.Status}}' "${SQLSERVER_CONTAINER}")" != "running" ]]; then
  docker start "${SQLSERVER_CONTAINER}" >/dev/null
  pass "${SQLSERVER_CONTAINER} started"
else
  pass "${SQLSERVER_CONTAINER} already running"
fi

echo "==> Waiting for SQL Server to accept connections"
SQLCMD_BIN=""
for i in $(seq 1 24); do
  SQLCMD_BIN="$(docker exec "${SQLSERVER_CONTAINER}" bash -c 'command -v sqlcmd || ls /opt/mssql-tools*/bin/sqlcmd 2>/dev/null | head -1' 2>/dev/null)"
  if [[ -n "${SQLCMD_BIN}" ]] && docker exec "${SQLSERVER_CONTAINER}" "${SQLCMD_BIN}" -S localhost -U sa -P "${SA_PASSWORD}" -C -Q "SELECT 1" >/dev/null 2>&1; then
    pass "SQL Server accepting connections"
    break
  fi
  sleep 5
done
[[ -z "${SQLCMD_BIN}" ]] && { fail "sqlcmd not found in ${SQLSERVER_CONTAINER}"; exit 1; }

echo "==> Ensuring ${CLAIMS_DB}.claims exists with sample data"
docker exec "${SQLSERVER_CONTAINER}" "${SQLCMD_BIN}" -S localhost -U sa -P "${SA_PASSWORD}" -C -Q "
IF NOT EXISTS (SELECT 1 FROM sys.databases WHERE name = '${CLAIMS_DB}')
  CREATE DATABASE ${CLAIMS_DB};
" >/dev/null
docker exec "${SQLSERVER_CONTAINER}" "${SQLCMD_BIN}" -S localhost -U sa -P "${SA_PASSWORD}" -C -d "${CLAIMS_DB}" -Q "
IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'claims')
BEGIN
  CREATE TABLE claims (
    claim_id BIGINT IDENTITY(1,1) PRIMARY KEY,
    customer_id INT NOT NULL,
    claim_amount DECIMAL(10,2) NOT NULL,
    claim_date DATETIME2 NOT NULL DEFAULT SYSDATETIME(),
    claim_status VARCHAR(50) NOT NULL DEFAULT 'PENDING',
    description VARCHAR(500)
  );
  INSERT INTO claims (customer_id, claim_amount, claim_status, description) VALUES
    (1001, 500.00, 'PENDING', 'Vehicle damage claim'),
    (1002, 1250.50, 'APPROVED', 'Medical claim'),
    (1003, 750.25, 'PENDING', 'Property damage claim');
END
" >/dev/null
pass "${CLAIMS_DB}.claims present"

step "5. SQL Server reverse tunnel (CRC node -> Docker Desktop)"
# Same GatewayPorts requirement/fix as the KDC tunnel in
# scripts/kerberos/setup-kerberos.sh - see docs/README-kerberos-authentication.md
# section 4.1 for why this is required, not optional.
if "${CRC_SSH[@]}" "sudo grep -qx 'GatewayPorts yes' /etc/ssh/sshd_config" 2>/dev/null; then
  pass "GatewayPorts already enabled"
else
  "${CRC_SSH[@]}" "sudo sed -i 's/^#\?GatewayPorts.*/GatewayPorts yes/' /etc/ssh/sshd_config; grep -qx 'GatewayPorts yes' /etc/ssh/sshd_config || echo 'GatewayPorts yes' | sudo tee -a /etc/ssh/sshd_config >/dev/null; sudo systemctl restart sshd"
  pkill -f "ssh.*-R 0.0.0.0:${SQLSERVER_PORT}" 2>/dev/null || true
  pass "GatewayPorts enabled and sshd restarted"
fi

NODE_IP="$(oc get nodes -o jsonpath='{.items[0].status.addresses[?(@.type=="InternalIP")].address}')"
sql_tunnel_is_healthy() {
  CONNECT_POD="$(oc get pod -l app=connect -n confluent -o jsonpath='{.items[0].metadata.name}' 2>/dev/null)"
  [[ -n "${CONNECT_POD}" ]] && oc exec "${CONNECT_POD}" -n confluent -c connect -- bash -c "exec 3<>/dev/tcp/${NODE_IP}/${SQLSERVER_PORT}" >/dev/null 2>&1
}
if sql_tunnel_is_healthy; then
  pass "SQL Server tunnel already up and reachable from Connect's pod"
else
  echo "==> (Re)starting the SQL Server tunnel"
  pkill -f "ssh.*-R 0.0.0.0:${SQLSERVER_PORT}:127.0.0.1:1433" 2>/dev/null || true
  sleep 1
  nohup ssh -i "$HOME/.crc/machines/crc/id_ed25519" -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o ServerAliveInterval=15 -o ServerAliveCountMax=3 -p 2222 -N \
    -R "0.0.0.0:${SQLSERVER_PORT}:127.0.0.1:1433" core@127.0.0.1 >/tmp/sql-tunnel.log 2>&1 &
  disown
  sleep 3
  # Health check here only confirms the node itself can reach it (no
  # connect pod may exist yet on a first-ever run) - the Kerberos script's
  # own tunnel checks re-verify from the pod once Connect is up.
  "${CRC_SSH[@]}" "nc -zv -w3 localhost ${SQLSERVER_PORT}" >/dev/null 2>&1 && pass "SQL Server tunnel up (node-local check)" || fail "SQL Server tunnel still not reachable - check the SSH connection to the CRC VM manually"
fi

step "6. Confluent Platform (KRaft, Kafka, Schema Registry, Connect, Control Center, REST Proxy)"
oc apply -k overlays/local
echo "==> Waiting for all platform pods to become Ready (this can take several minutes on first run)"
for cr in kraftcontroller kafka schemaregistry connect controlcenter kafkarestproxy; do
  oc wait pod -n confluent -l "app=${cr}" --for=condition=Ready --timeout=600s 2>&1 || fail "${cr} pod not Ready after 600s - check 'oc get pods -n confluent'"
done
pass "platform pods Ready"

step "7. Kerberos: AD DC, keytabs, SQL Server domain-join, connector"
SQLSERVER_HOST="${NODE_IP}" SQLSERVER_PORT="${SQLSERVER_PORT}" SQLSERVER_CONTAINER="${SQLSERVER_CONTAINER}" SA_PASSWORD="${SA_PASSWORD}" CLAIMS_DB="${CLAIMS_DB}" \
  ./scripts/kerberos/setup-kerberos.sh

step "8. Hand off to Argo CD for ongoing GitOps self-heal"
oc apply -f apps/app-of-apps.yaml >/dev/null 2>&1 || true

step "Summary"
./scripts/kerberos/validate-kerberos.sh || true
cat <<EOF

  Control Center:    https://controlcenter.apps-crc.testing
  Schema Registry:   https://schemaregistry.apps-crc.testing
  Kafka Connect:     https://connect.apps-crc.testing
  Kafka REST Proxy:  https://kafkarestproxy.apps-crc.testing

Re-run this whole script any time - every step re-checks state before acting.
EOF
