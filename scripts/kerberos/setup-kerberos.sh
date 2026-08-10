#!/usr/bin/env bash
# Single end-to-end setup for Kerberos-authenticated SQL Server access
# from Kafka Connect: provisions the Samba4 AD domain controller in
# Docker Desktop, joins SQL Server to it, opens the reverse SSH tunnel
# Connect needs to reach the DC, exports/seals keytabs, builds the Schema
# Registry truststore, creates the Kafka topic, and registers the
# connector. Idempotent throughout - safe to re-run in full any time
# (e.g. after a Mac reboot or `crc stop`/`crc start`, neither of which any
# of this survives - see docs/kerberos-architecture.md).
#
# Requires SQLSERVER_HOST/SQLSERVER_PORT: the address Connect uses to
# reach SQL Server (an existing reverse tunnel into its Docker Desktop
# container - this script does not create that one, only the AD DC's).
# Requires SQLSERVER_CONTAINER (default sqltest2): the SQL Server
# container's name.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
NETWORK="kerberos-net"
SAMBA_CONTAINER="sambaad"
SAMBA_VOLUME="sambaad-data"
SQLSERVER_CONTAINER="${SQLSERVER_CONTAINER:-sqltest2}"
SQLSERVER_HOST="${SQLSERVER_HOST:?Set SQLSERVER_HOST to the address Connect uses to reach SQL Server}"
SQLSERVER_PORT="${SQLSERVER_PORT:-1433}"
ADMIN_PASSWORD="${SAMBA_ADMIN_PASSWORD:-SambaAdmin@Psyncopate2024!}"
REALM="PSYNCOPATE.COM"
NODE_TUNNEL_PORT="${NODE_TUNNEL_PORT:-18088}"
SAMBA_HOST_PORT="${SAMBA_HOST_PORT:-8088}"
# SQL Server's sa password and the database/AD-account the JDBC connector logs
# in as - see base/confluent-platform/sqlserver-claims-topic.yaml and
# scripts/kerberos/setup-kerberos.sh's own connector registration below.
SA_PASSWORD="${SA_PASSWORD:-YourPassword123!}"
CLAIMS_DB="${CLAIMS_DB:-claims_db}"
AD_LOGIN='PSYNCOPATE\connect-svc'
# Reused everywhere this script needs to run something on the CRC VM itself
# (not through `oc`/a pod) - e.g. sshd_config edits, reverse tunnels.
CRC_SSH=(ssh -i "$HOME/.crc/machines/crc/id_ed25519" -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -p 2222 core@127.0.0.1)

pass() { echo "  OK: $1"; }
fail() { echo "  FAIL: $1"; }

echo "############################################################"
echo "# 1. Samba AD domain controller (Docker Desktop)"
echo "############################################################"

echo "==> Ensuring ${NETWORK} network exists"
docker network inspect "${NETWORK}" >/dev/null 2>&1 || docker network create "${NETWORK}"

echo "==> Ensuring ${SAMBA_VOLUME} volume exists (holds the provisioned domain across recreates)"
docker volume inspect "${SAMBA_VOLUME}" >/dev/null 2>&1 || docker volume create "${SAMBA_VOLUME}"

# The container's own command is the full apt-get+provision-if-needed+exec
# sequence below, run as PID 1 under --restart=unless-stopped. This is
# deliberate, not incidental: confirmed live that a samba process merely
# backgrounded via `docker exec -d ... nohup samba &` can die (crash, or
# just get reaped) with nothing to restart it, silently breaking Connect
# with "Cannot get a KDC reply" until someone notices and manually
# restarts it. Making samba PID 1 means Docker's own restart policy
# recovers it automatically - apt-get re-runs on every crash-restart
# (wasteful, ~20-40s, but simple and correct), while /var/lib/samba on
# the named volume means the provisioned domain itself is never re-done.
SAMBA_CMD='
set -e
export DEBIAN_FRONTEND=noninteractive
apt-get update -qq
apt-get install -y -qq samba samba-common-bin krb5-user winbind smbclient ldb-tools

if [ ! -f /var/lib/samba/private/sam.ldb ]; then
  echo "==> No existing domain - provisioning '"${REALM}"'"
  rm -f /etc/samba/smb.conf
  samba-tool domain provision \
    --use-rfc2307 \
    --realm='"${REALM}"' \
    --domain=PSYNCOPATE \
    --server-role=dc \
    --dns-backend=SAMBA_INTERNAL \
    --host-name=SAMBADC1 \
    --option="vfs objects = dfs_samba4 acl_xattr xattr_tdb" \
    --adminpass="'"${ADMIN_PASSWORD}"'"
  cp /etc/samba/smb.conf /var/lib/samba/smb.conf.persisted
else
  # smb.conf itself lives outside /var/lib/samba, so it does not survive
  # a container recreation even though the domain data does - confirmed
  # live that skipping this restore makes the freshly apt-installed
  # default smb.conf (server role = standalone) reject starting as an AD
  # DC: "Samba detected misconfigured '"'"'server role'"'"' and exited".
  echo "==> Existing domain found - restoring smb.conf"
  cp /var/lib/samba/smb.conf.persisted /etc/samba/smb.conf
fi

cat > /etc/krb5.conf <<KRB5EOF
[libdefaults]
    default_realm = '"${REALM}"'
    dns_lookup_realm = false
    dns_lookup_kdc = false
    udp_preference_limit = 1

[realms]
    '"${REALM}"' = {
        kdc = 127.0.0.1:88
        admin_server = 127.0.0.1:464
        default_domain = psyncopate.com
    }

[domain_realm]
    .psyncopate.com = '"${REALM}"'
    psyncopate.com = '"${REALM}"'
KRB5EOF

exec samba -i --debug-stdout
'

if ! docker inspect "${SAMBA_CONTAINER}" >/dev/null 2>&1; then
  echo "==> Creating ${SAMBA_CONTAINER}"
  docker run -d --name "${SAMBA_CONTAINER}" --hostname SAMBADC1 --restart=unless-stopped \
    --network "${NETWORK}" -p "${SAMBA_HOST_PORT}:88" \
    -v "${SAMBA_VOLUME}:/var/lib/samba" \
    debian:12-slim bash -c "${SAMBA_CMD}"
elif [[ "$(docker inspect -f '{{.State.Status}}' "${SAMBA_CONTAINER}")" != "running" ]]; then
  echo "==> ${SAMBA_CONTAINER} exists but isn't running - starting it"
  docker start "${SAMBA_CONTAINER}"
fi

echo "==> Waiting for the domain controller to come up (provisioning takes a minute on first run)"
for i in $(seq 1 30); do
  if docker exec "${SAMBA_CONTAINER}" bash -c "echo '${ADMIN_PASSWORD}' | kinit administrator@${REALM}" >/dev/null 2>&1; then
    pass "kinit succeeded - domain controller is healthy"
    break
  fi
  if [[ "${i}" -eq 30 ]]; then
    fail "domain controller never became healthy - check: docker logs ${SAMBA_CONTAINER}"
    exit 1
  fi
  sleep 5
done

echo
echo "############################################################"
echo "# 2. AD accounts"
echo "############################################################"

docker exec "${SAMBA_CONTAINER}" samba-tool user create connect-svc "ConnectSvc@Psyncopate2024!" -U "administrator%${ADMIN_PASSWORD}" 2>/dev/null || echo "  (connect-svc already exists)"
docker exec "${SAMBA_CONTAINER}" samba-tool user create mssql-svc "MssqlSvc@Psyncopate2024!" -U "administrator%${ADMIN_PASSWORD}" 2>/dev/null || echo "  (mssql-svc already exists)"
docker exec "${SAMBA_CONTAINER}" samba-tool spn add "MSSQLSvc/${SQLSERVER_HOST}:${SQLSERVER_PORT}" mssql-svc -U "administrator%${ADMIN_PASSWORD}" 2>/dev/null || echo "  (SPN already attached)"
pass "connect-svc, mssql-svc accounts present"

echo
echo "############################################################"
echo "# 3. Export and seal keytabs"
echo "############################################################"

KEYTAB_DIR="${REPO_ROOT}/.kerberos-keytabs"
mkdir -p "${KEYTAB_DIR}"

docker exec "${SAMBA_CONTAINER}" rm -f /tmp/connect.keytab /tmp/mssql.keytab
docker exec "${SAMBA_CONTAINER}" samba-tool domain exportkeytab /tmp/connect.keytab --principal="connect-svc@${REALM}"
docker cp "${SAMBA_CONTAINER}:/tmp/connect.keytab" "${KEYTAB_DIR}/connect.keytab"
docker exec "${SAMBA_CONTAINER}" samba-tool domain exportkeytab /tmp/mssql.keytab --principal="MSSQLSvc/${SQLSERVER_HOST}:${SQLSERVER_PORT}@${REALM}"
docker cp "${SAMBA_CONTAINER}:/tmp/mssql.keytab" "${KEYTAB_DIR}/mssql.keytab"
docker exec "${SAMBA_CONTAINER}" rm -f /tmp/connect.keytab /tmp/mssql.keytab

CERT_PATH="/tmp/sealed-secrets-public-cert.pem"
if [[ -f "${CERT_PATH}" ]]; then
  CONNECT_KEYTAB_B64="$(base64 < "${KEYTAB_DIR}/connect.keytab" | tr -d '\n')"
  kubeseal --cert "${CERT_PATH}" --format yaml <<EOF > "${REPO_ROOT}/base/confluent-platform/secrets/connect-keytab-sealed.yaml"
apiVersion: v1
kind: Secret
metadata:
  name: connect-keytab
  namespace: confluent
type: Opaque
data:
  connect.keytab: ${CONNECT_KEYTAB_B64}
EOF
  pass "connect.keytab sealed into base/confluent-platform/secrets/connect-keytab-sealed.yaml"
  rm -f "${KEYTAB_DIR}/connect.keytab"
  oc apply -f "${REPO_ROOT}/base/confluent-platform/secrets/connect-keytab-sealed.yaml" >/dev/null 2>&1 || true
  echo "  Remember to commit+push this file so Argo CD's self-heal doesn't revert it."
else
  fail "no sealed-secrets cert at ${CERT_PATH} - connect.keytab left as plaintext in ${KEYTAB_DIR}/, seal it manually"
fi

echo
echo "############################################################"
echo "# 4. Join SQL Server to the domain"
echo "############################################################"

docker network connect "${NETWORK}" "${SQLSERVER_CONTAINER}" 2>/dev/null || true
SAMBA_IP="$(docker inspect "${SAMBA_CONTAINER}" --format "{{(index .NetworkSettings.Networks \"${NETWORK}\").IPAddress}}")"
SQLSERVER_HOSTNAME="$(docker exec "${SQLSERVER_CONTAINER}" hostname)"
COMPUTER_NAME="$(echo "${SQLSERVER_HOSTNAME}" | tr '[:lower:]' '[:upper:]' | cut -c1-15)"

echo "==> Pointing ${SQLSERVER_CONTAINER}'s DNS at the AD DC"
docker exec -u root "${SQLSERVER_CONTAINER}" bash -c "
echo 'nameserver ${SAMBA_IP}' > /etc/resolv.conf
echo 'search psyncopate.com' >> /etc/resolv.conf
grep -q sambadc1 /etc/hosts || echo '${SAMBA_IP} sambadc1.psyncopate.com SAMBADC1.psyncopate.com sambaad' >> /etc/hosts
"

echo "==> Writing krb5.conf/smb.conf"
docker exec -u root "${SQLSERVER_CONTAINER}" bash -c "
cat > /etc/krb5.conf <<KRB5EOF
[libdefaults]
    default_realm = ${REALM}
    dns_lookup_realm = false
    dns_lookup_kdc = false
    rdns = false

[realms]
    ${REALM} = {
        kdc = sambaad:88
        admin_server = sambaad:464
        default_domain = psyncopate.com
    }

[domain_realm]
    .psyncopate.com = ${REALM}
    psyncopate.com = ${REALM}
KRB5EOF

mkdir -p /etc/samba
cat > /etc/samba/smb.conf <<SMBEOF
[global]
    workgroup = PSYNCOPATE
    realm = ${REALM}
    security = ads
    password server = sambaad
    kerberos method = secrets and keytab
SMBEOF
"

echo "==> Creating/refreshing the SQL Server computer account and its keytab"
# net ads join's own post-join self-check is unreliable here (confirmed
# live: NT_STATUS_NO_TRUST_SAM_ACCOUNT even for a genuinely valid trust
# account) - creating the account directly and exporting its keytab
# sidesteps it, since sssd only needs the keytab.
docker exec "${SAMBA_CONTAINER}" samba-tool computer create "${COMPUTER_NAME}" -U "administrator%${ADMIN_PASSWORD}" 2>/dev/null || true
docker exec "${SAMBA_CONTAINER}" samba-tool user setpassword "${COMPUTER_NAME}\$" --newpassword="HostSvc@Psyncopate2024!" -U "administrator%${ADMIN_PASSWORD}"
docker exec "${SAMBA_CONTAINER}" samba-tool user enable "${COMPUTER_NAME}\$" -U "administrator%${ADMIN_PASSWORD}"
# FQDN, lowercase, throughout - sssd's own ad_hostname setting below makes
# it kinit as the lowercase FQDN form for its LDAP bind, and Kerberos
# principal matching is case-sensitive (confirmed live: a keytab exported
# for the uppercase form net ads join happened to leave behind earlier
# fails with "no suitable keys" against a lowercase request).
HOST_PRINC="host/${SQLSERVER_HOSTNAME}.psyncopate.com@${REALM}"
docker exec "${SAMBA_CONTAINER}" samba-tool spn add "HOST/${SQLSERVER_HOSTNAME}.psyncopate.com" "${COMPUTER_NAME}\$" -U "administrator%${ADMIN_PASSWORD}" 2>/dev/null || true
# sssd kinits using its own account's *identity*, not an SPN string - an
# SPN alone (like a service's SPN) isn't a valid client principal for
# kinit (same limitation as connect-svc's own SPN, see the header
# comment above). net ads join sets the computer account's
# userPrincipalName to match its own host/ SPN automatically; creating
# the account directly via samba-tool never does, so kinit fails with
# "Client not found in Kerberos database" until this is set explicitly.
docker exec "${SAMBA_CONTAINER}" bash -c "
cat > /tmp/setupn.ldif <<EOF
dn: CN=${COMPUTER_NAME},CN=Computers,DC=psyncopate,DC=com
changetype: modify
replace: userPrincipalName
userPrincipalName: ${HOST_PRINC}
EOF
ldbmodify -H /var/lib/samba/private/sam.ldb /tmp/setupn.ldif
rm -f /tmp/setupn.ldif
"
docker exec "${SAMBA_CONTAINER}" rm -f /tmp/host.keytab
docker exec "${SAMBA_CONTAINER}" samba-tool domain exportkeytab /tmp/host.keytab --principal="${HOST_PRINC}"
docker cp "${SAMBA_CONTAINER}:/tmp/host.keytab" /tmp/host.keytab
docker cp /tmp/host.keytab "${SQLSERVER_CONTAINER}:/etc/krb5.keytab"
docker exec "${SAMBA_CONTAINER}" rm -f /tmp/host.keytab
rm -f /tmp/host.keytab
docker exec -u root "${SQLSERVER_CONTAINER}" chmod 600 /etc/krb5.keytab

echo "==> Configuring and starting sssd"
docker exec -u root "${SQLSERVER_CONTAINER}" bash -c "
which sssd >/dev/null 2>&1 || (apt-get update -qq && apt-get install -y -qq realmd sssd-ad sssd-tools adcli samba-common-bin krb5-user)
cat > /etc/sssd/sssd.conf <<SSSDEOF
[sssd]
logger = files
domains = psyncopate.com
config_file_version = 2
services = nss, pam

[domain/psyncopate.com]
id_provider = ad
access_provider = ad
ad_domain = psyncopate.com
ad_server = sambadc1.psyncopate.com
ad_hostname = ${SQLSERVER_HOSTNAME}.psyncopate.com
override_homedir = /home/%u
SSSDEOF
chmod 600 /etc/sssd/sssd.conf
grep -q ' sss' /etc/nsswitch.conf || sed -i -E 's/^(passwd|group):(.*)\$/\1:\2 sss/' /etc/nsswitch.conf
pkill -9 sssd 2>/dev/null || true
sleep 1
rm -rf /var/lib/sss/db/*
rm -f /run/sssd.pid
/usr/sbin/sssd -D --logger=files
sleep 4
"

echo "==> Mounting mssql.keytab and enabling Kerberos in mssql-conf"
if [[ ! -f "${KEYTAB_DIR}/mssql.keytab" ]]; then
  fail "${KEYTAB_DIR}/mssql.keytab not found - step 3 above should have created it"
  exit 1
fi
docker cp "${KEYTAB_DIR}/mssql.keytab" "${SQLSERVER_CONTAINER}:/var/opt/mssql/secrets/mssql.keytab"
docker exec -u root "${SQLSERVER_CONTAINER}" chown mssql:root /var/opt/mssql/secrets/mssql.keytab
docker exec -u root "${SQLSERVER_CONTAINER}" chmod 400 /var/opt/mssql/secrets/mssql.keytab
docker exec -u root "${SQLSERVER_CONTAINER}" /opt/mssql/bin/mssql-conf set network.kerberoskeytabfile /var/opt/mssql/secrets/mssql.keytab

if docker exec "${SQLSERVER_CONTAINER}" getent passwd connect-svc@psyncopate.com >/dev/null 2>&1; then
  pass "sssd resolves connect-svc@psyncopate.com - SQL Server trusts the domain"
else
  fail "sssd cannot resolve AD identities yet"
fi

echo "  NOTE: restart SQL Server to load the new keytab if this is a fresh join:"
echo "    docker restart ${SQLSERVER_CONTAINER}"
echo "  Re-run this whole script after every docker restart ${SQLSERVER_CONTAINER} -"
echo "  Docker wipes /etc/resolv.conf, /etc/hosts, and kills sssd on restart."

echo "==> Ensuring SQL Server has a login/user for ${AD_LOGIN} (Kerberos-authenticated AD account the connector logs in as)"
# Confirmed live: Kerberos auth alone gets the connector past the KDC/SQL
# Server handshake, but SQL Server still rejects the connection with
# "Login failed for user 'PSYNCOPATE\connect-svc'" until this Windows login
# and a matching database user/role exist - that step is SQL-side
# authorization, separate from (and not implied by) the domain trust set up
# above. Not gated behind the fresh-join branch above since a restarted
# container keeps its logins/users (only sssd/DNS/hosts get wiped).
SQLCMD_BIN="$(docker exec "${SQLSERVER_CONTAINER}" bash -c 'command -v sqlcmd || ls /opt/mssql-tools*/bin/sqlcmd 2>/dev/null | head -1')"
if [[ -z "${SQLCMD_BIN}" ]]; then
  fail "sqlcmd not found in ${SQLSERVER_CONTAINER} - can't create SQL login for ${AD_LOGIN}"
else
  docker exec "${SQLSERVER_CONTAINER}" "${SQLCMD_BIN}" -S localhost -U sa -P "${SA_PASSWORD}" -C -Q "
    IF NOT EXISTS (SELECT 1 FROM sys.server_principals WHERE name = '${AD_LOGIN}')
      CREATE LOGIN [${AD_LOGIN}] FROM WINDOWS;
  " >/dev/null 2>&1 || true
  docker exec "${SQLSERVER_CONTAINER}" "${SQLCMD_BIN}" -S localhost -U sa -P "${SA_PASSWORD}" -C -d "${CLAIMS_DB}" -Q "
    IF NOT EXISTS (SELECT 1 FROM sys.database_principals WHERE name = '${AD_LOGIN}')
      CREATE USER [${AD_LOGIN}] FOR LOGIN [${AD_LOGIN}];
    IF NOT EXISTS (SELECT 1 FROM sys.database_role_members rm JOIN sys.database_principals r ON rm.role_principal_id = r.principal_id JOIN sys.database_principals m ON rm.member_principal_id = m.principal_id WHERE r.name = 'db_datareader' AND m.name = '${AD_LOGIN}')
      ALTER ROLE db_datareader ADD MEMBER [${AD_LOGIN}];
  " >/dev/null 2>&1 \
    && pass "${AD_LOGIN} has a SQL login + db_datareader on ${CLAIMS_DB}" \
    || fail "could not create SQL login/user for ${AD_LOGIN} - does database '${CLAIMS_DB}' exist yet? Check SA_PASSWORD too."
fi

echo
echo "############################################################"
echo "# 5. Connect-side reverse tunnels (cluster -> Docker Desktop)"
echo "############################################################"

echo "==> Ensuring sshd's GatewayPorts is enabled on the CRC node"
# Confirmed live: without this, sshd silently binds every `-R 0.0.0.0:PORT:...`
# reverse forward to loopback only (127.0.0.1/::1) regardless of what the
# client asked for, so this tunnel (and the separately-managed SQL Server
# one on SQLSERVER_PORT) are reachable from the Mac/CRC-node-itself but not
# from pods, which connect via the node's real IP - JDBC then fails with
# "Connection refused" even though the tunnel process looks healthy.
GATEWAY_PORTS_CHANGED=false
if "${CRC_SSH[@]}" "sudo grep -qx 'GatewayPorts yes' /etc/ssh/sshd_config" 2>/dev/null; then
  pass "GatewayPorts already enabled"
else
  "${CRC_SSH[@]}" "sudo sed -i 's/^#\?GatewayPorts.*/GatewayPorts yes/' /etc/ssh/sshd_config; grep -qx 'GatewayPorts yes' /etc/ssh/sshd_config || echo 'GatewayPorts yes' | sudo tee -a /etc/ssh/sshd_config >/dev/null; sudo systemctl restart sshd"
  GATEWAY_PORTS_CHANGED=true
  pass "GatewayPorts enabled and sshd restarted"
fi
if [[ "${GATEWAY_PORTS_CHANGED}" == true ]]; then
  # sshd restarting doesn't kill already-established forwarded-port sessions,
  # so those need to be force-restarted to actually pick up the new setting.
  echo "  Restarting this script's own KDC tunnel (any other manually-run"
  echo "  tunnels, e.g. the SQL Server one, need restarting too - re-run"
  echo "  whatever command established SQLSERVER_HOST:SQLSERVER_PORT)"
  pkill -f "ssh.*-R 0.0.0.0:${NODE_TUNNEL_PORT}" 2>/dev/null || true
fi

echo "==> Ensuring the node-side iptables DROP rule for UDP ${NODE_TUNNEL_PORT} is present"
# Java's krb5 client tries UDP first regardless of udp_preference_limit
# (a real JDK quirk, confirmed live) - ssh -R can't carry UDP, so nothing
# listens on that port on the node, and an unanswered UDP send there gets
# an instant ICMP "port unreachable" that Java treats as fatal instead of
# falling back to TCP. Dropping (not rejecting) it makes Java's send
# silently time out instead, which *does* trigger the TCP fallback.
oc debug node/crc -- chroot /host bash -c "
  iptables -C INPUT -p udp --dport ${NODE_TUNNEL_PORT} -j DROP 2>/dev/null || iptables -I INPUT -p udp --dport ${NODE_TUNNEL_PORT} -j DROP
" 2>&1 | grep -v "^Starting pod\|^Removing debug pod\|^To use host binaries"

echo "==> Checking tunnel health (actual port response, not just a matching process)"
tunnel_is_healthy() {
  NODE_IP="$(oc get nodes -o jsonpath='{.items[0].status.addresses[?(@.type=="InternalIP")].address}')"
  CONNECT_POD="$(oc get pod -l app=connect -n confluent -o jsonpath='{.items[0].metadata.name}' 2>/dev/null)"
  [[ -n "${CONNECT_POD}" ]] && oc exec "${CONNECT_POD}" -n confluent -c connect -- bash -c "exec 3<>/dev/tcp/${NODE_IP}/${NODE_TUNNEL_PORT}" >/dev/null 2>&1
}

if tunnel_is_healthy; then
  pass "tunnel already up and reachable from Connect's pod"
else
  echo "==> (Re)starting the tunnel"
  pkill -f "ssh.*-R 0.0.0.0:${NODE_TUNNEL_PORT}:localhost:${SAMBA_HOST_PORT}" 2>/dev/null || true
  sleep 1
  ssh -i ~/.crc/machines/crc/id_ed25519 -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -p 2222 -N \
    -R "0.0.0.0:${NODE_TUNNEL_PORT}:localhost:${SAMBA_HOST_PORT}" core@127.0.0.1 &
  disown
  sleep 3
  tunnel_is_healthy && pass "tunnel now up" || fail "tunnel still not reachable - check the SSH connection to the CRC VM manually"
fi

echo
echo "############################################################"
echo "# 6. Schema Registry truststore"
echo "############################################################"

WORK_DIR="$(mktemp -d)"
trap 'rm -rf "${WORK_DIR}"' EXIT
oc get secret schemaregistry-tls-secret -n confluent -o jsonpath='{.data.ca\.crt}' | base64 -d > "${WORK_DIR}/sr-ca.crt"
keytool -importcert -noprompt -alias schemaregistry-ca \
  -file "${WORK_DIR}/sr-ca.crt" -keystore "${WORK_DIR}/truststore.jks" -storepass changeit
TRUSTSTORE_B64="$(base64 < "${WORK_DIR}/truststore.jks" | tr -d '\n')"
cat > "${REPO_ROOT}/base/confluent-platform/connect-truststore-configmap.yaml" <<EOF
# JKS truststore containing Schema Registry's CA cert, so Connect's JVM
# trusts its TLS certificate when the JDBC Source Connector's Avro
# serializer calls it. Regenerated by scripts/kerberos/setup-kerberos.sh -
# rerun that if schemaregistry-tls-secret's CA ever rotates.
apiVersion: v1
kind: ConfigMap
metadata:
  name: connect-truststore
  namespace: confluent
binaryData:
  truststore.jks: ${TRUSTSTORE_B64}
EOF
oc apply -f "${REPO_ROOT}/base/confluent-platform/connect-truststore-configmap.yaml" >/dev/null 2>&1 || true
pass "truststore built and applied"

echo
echo "############################################################"
echo "# 7. Register the connector"
echo "############################################################"

CONNECT_URL="${CONNECT_URL:-https://connect.apps-crc.testing}"
CONNECTOR_NAME="sqlserver-claims-source"
curl -sk -X PUT "${CONNECT_URL}/connectors/${CONNECTOR_NAME}/config" \
  -H "Content-Type: application/json" \
  -d '{
    "connector.class": "io.confluent.connect.jdbc.JdbcSourceConnector",
    "connection.url": "jdbc:sqlserver://'"${SQLSERVER_HOST}"':'"${SQLSERVER_PORT}"';databaseName=claims_db;integratedSecurity=true;authenticationScheme=JavaKerberos;encrypt=false;",
    "table.whitelist": "claims",
    "mode": "timestamp+incrementing",
    "incrementing.column.name": "claim_id",
    "timestamp.column.name": "claim_date",
    "topic.prefix": "sqlserver-",
    "poll.interval.ms": "10000",
    "tasks.max": "1",
    "key.converter": "org.apache.kafka.connect.storage.StringConverter",
    "value.converter": "io.confluent.connect.avro.AvroConverter",
    "value.converter.schema.registry.url": "https://schemaregistry.confluent.svc.cluster.local:8081"
  }' >/dev/null

sleep 10
STATUS_JSON="$(curl -sk "${CONNECT_URL}/connectors/${CONNECTOR_NAME}/status")"
echo "${STATUS_JSON}" | python3 -m json.tool 2>/dev/null || echo "${STATUS_JSON}"

echo
echo "############################################################"
echo "# Summary"
echo "############################################################"
if echo "${STATUS_JSON}" | grep -q '"state":"RUNNING"'; then
  pass "connector RUNNING"
else
  fail "connector not RUNNING yet - see trace above. Often just needs a retry:"
  echo "    curl -sk -X POST \"${CONNECT_URL}/connectors/${CONNECTOR_NAME}/restart?includeTasks=true&onlyFailed=false\""
fi
