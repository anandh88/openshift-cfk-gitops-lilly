#!/usr/bin/env bash
# Joins the SQL Server container to PSYNCOPATE.COM and mounts its keytab -
# both required for SQL Server to trust Kerberos logins at all. Confirmed
# live: a valid keytab alone gets the GSSAPI ticket decrypted correctly,
# but SQL Server's login-mapping additionally calls into the OS identity
# stack (sssd's ad provider) to resolve the client principal to a trusted
# domain account, and rejects the login as "from an untrusted domain"
# without it.
#
# Not idempotent against a full `docker restart` of the SQL Server
# container - Docker regenerates /etc/resolv.conf and /etc/hosts on
# restart (a Docker platform behavior, not something this script
# controls), which drops the DNS-points-at-the-DC and hosts-file changes
# below, and kills the backgrounded sssd process. Re-run this whole script
# after any `docker restart sqltest2`.
set -euo pipefail

SAMBA_CONTAINER="sambaad"
SQLSERVER_CONTAINER="${SQLSERVER_CONTAINER:-sqltest2}"
ADMIN_PASSWORD="${SAMBA_ADMIN_PASSWORD:-SambaAdmin@Psyncopate2024!}"
REALM="PSYNCOPATE.COM"

echo "==> Attaching ${SQLSERVER_CONTAINER} to the kerberos-net network"
docker network connect kerberos-net "${SQLSERVER_CONTAINER}" 2>/dev/null || true
SAMBA_IP="$(docker inspect "${SAMBA_CONTAINER}" --format '{{(index .NetworkSettings.Networks "kerberos-net").IPAddress}}')"
SQLSERVER_HOSTNAME="$(docker exec "${SQLSERVER_CONTAINER}" hostname)"

echo "==> Pointing DNS at the AD DC (needed for the SRV lookups CLDAP discovery depends on)"
docker exec -u root "${SQLSERVER_CONTAINER}" bash -c "
echo 'nameserver ${SAMBA_IP}' > /etc/resolv.conf
echo 'search psyncopate.com' >> /etc/resolv.conf
grep -q sambadc1 /etc/hosts || echo '${SAMBA_IP} sambadc1.psyncopate.com SAMBADC1.psyncopate.com sambaad' >> /etc/hosts
"

echo "==> Writing krb5.conf/smb.conf"
docker exec -u root "${SQLSERVER_CONTAINER}" bash -c "
cat > /etc/krb5.conf <<EOF
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
EOF

cat > /etc/samba/smb.conf <<EOF
[global]
    workgroup = PSYNCOPATE
    realm = ${REALM}
    security = ads
    password server = sambaad
    kerberos method = secrets and keytab
EOF
"

echo "==> Creating/refreshing the computer account and its keytab on the AD DC"
# net ads join's own post-join self-check ("verify domain membership")
# is unreliable in this setup - confirmed live it sometimes fails with
# NT_STATUS_NO_TRUST_SAM_ACCOUNT even though the trust account it just
# created is genuinely valid (enabled, password set). Creating the
# account directly via samba-tool and exporting its keytab sidesteps that
# unreliable self-check entirely - sssd only needs the keytab, not a
# successful `net ads join` run.
COMPUTER_NAME="$(echo "${SQLSERVER_HOSTNAME}" | tr '[:lower:]' '[:upper:]' | cut -c1-15)"
docker exec "${SAMBA_CONTAINER}" samba-tool computer create "${COMPUTER_NAME}" -U "administrator%${ADMIN_PASSWORD}" 2>/dev/null || true
docker exec "${SAMBA_CONTAINER}" samba-tool user setpassword "${COMPUTER_NAME}\$" --newpassword="HostSvc@Psyncopate2024!" -U "administrator%${ADMIN_PASSWORD}"
docker exec "${SAMBA_CONTAINER}" samba-tool user enable "${COMPUTER_NAME}\$" -U "administrator%${ADMIN_PASSWORD}"
docker exec "${SAMBA_CONTAINER}" samba-tool spn add "HOST/${SQLSERVER_HOSTNAME}" "${COMPUTER_NAME}\$" -U "administrator%${ADMIN_PASSWORD}" 2>/dev/null || true
docker exec "${SAMBA_CONTAINER}" rm -f /tmp/host.keytab
docker exec "${SAMBA_CONTAINER}" samba-tool domain exportkeytab /tmp/host.keytab --principal="host/${SQLSERVER_HOSTNAME}@${REALM}"
docker cp "${SAMBA_CONTAINER}:/tmp/host.keytab" /tmp/host.keytab
docker cp /tmp/host.keytab "${SQLSERVER_CONTAINER}:/etc/krb5.keytab"
docker exec "${SAMBA_CONTAINER}" rm -f /tmp/host.keytab
rm -f /tmp/host.keytab
docker exec -u root "${SQLSERVER_CONTAINER}" chmod 600 /etc/krb5.keytab

echo "==> Configuring and starting sssd"
docker exec -u root "${SQLSERVER_CONTAINER}" bash -c "
which sssd >/dev/null 2>&1 || (apt-get update -qq && apt-get install -y -qq realmd sssd-ad sssd-tools adcli samba-common-bin krb5-user)
cat > /etc/sssd/sssd.conf <<EOF
[sssd]
logger = files
domains = ${REALM,,}
config_file_version = 2
services = nss, pam

[domain/${REALM,,}]
id_provider = ad
access_provider = ad
ad_domain = ${REALM,,}
ad_server = sambadc1.psyncopate.com
ad_hostname = ${SQLSERVER_HOSTNAME}.psyncopate.com
override_homedir = /home/%u
EOF
chmod 600 /etc/sssd/sssd.conf
grep -q ' sss' /etc/nsswitch.conf || sed -i -E 's/^(passwd|group):(.*)$/\1:\2 sss/' /etc/nsswitch.conf
pkill -9 sssd 2>/dev/null || true
sleep 1
rm -rf /var/lib/sss/db/*
rm -f /run/sssd.pid
/usr/sbin/sssd -D --logger=files
sleep 4
"

echo "==> Mounting mssql.keytab and enabling Kerberos in mssql-conf"
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
if [[ ! -f "${REPO_ROOT}/.kerberos-keytabs/mssql.keytab" ]]; then
  echo "ERROR: ${REPO_ROOT}/.kerberos-keytabs/mssql.keytab not found. Run scripts/kerberos/03-export-keytabs.sh first."
  exit 1
fi
docker cp "${REPO_ROOT}/.kerberos-keytabs/mssql.keytab" "${SQLSERVER_CONTAINER}:/var/opt/mssql/secrets/mssql.keytab"
docker exec -u root "${SQLSERVER_CONTAINER}" chown mssql:mssql /var/opt/mssql/secrets/mssql.keytab
docker exec -u root "${SQLSERVER_CONTAINER}" chmod 400 /var/opt/mssql/secrets/mssql.keytab
docker exec -u root "${SQLSERVER_CONTAINER}" /opt/mssql/bin/mssql-conf set network.kerberoskeytabfile /var/opt/mssql/secrets/mssql.keytab

echo "==> Verifying identity resolution"
docker exec "${SQLSERVER_CONTAINER}" getent passwd connect-svc@psyncopate.com

echo "==> Done. Restart SQL Server to load the new keytab: docker restart ${SQLSERVER_CONTAINER}"
echo "    Then create its SQL login once (see docs/kerberos-runbook.md)."
