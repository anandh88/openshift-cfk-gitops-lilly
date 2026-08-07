#!/usr/bin/env bash
# Provisions the Samba4 Active Directory Domain Controller for realm/domain
# PSYNCOPATE.COM/PSYNCOPATE as a Docker Desktop container, alongside SQL
# Server - not in the OpenShift cluster. See docs/kerberos-architecture.md
# for why: SQL Server's login-mapping (sssd's ad provider) needs full
# TCP+UDP connectivity to the DC (CLDAP pings, DNS SRV records) to trust a
# domain at all, which only works reliably when the DC and SQL Server share
# a real network - confirmed live that tunneling this traffic (ssh -L/-R
# can't carry UDP) hits an unresolvable wall.
#
# Idempotent: safe to re-run. Skips provisioning if a domain already
# exists on the container (detected via /var/lib/samba/private/sam.ldb),
# matching this repo's usual "check before creating" pattern.
set -euo pipefail

ADMIN_PASSWORD="${SAMBA_ADMIN_PASSWORD:-SambaAdmin@Psyncopate2024!}"
NETWORK="kerberos-net"
CONTAINER="sambaad"

echo "==> Ensuring ${NETWORK} exists"
docker network inspect "${NETWORK}" >/dev/null 2>&1 || docker network create "${NETWORK}"

if ! docker inspect "${CONTAINER}" >/dev/null 2>&1; then
  echo "==> Creating ${CONTAINER} container"
  # -p 8088:88: published so the cluster can reach Kerberos (88) via a
  # reverse SSH tunnel (see docs/kerberos-architecture.md) - Connect only
  # ever needs AS-REQ/TGS-REQ (TCP), never LDAP/SMB/kpasswd directly, so
  # this is the only port published to the host.
  docker run -d --name "${CONTAINER}" --hostname SAMBADC1 \
    --network "${NETWORK}" -p 8088:88 debian:12-slim sleep infinity
fi

echo "==> Installing samba packages"
docker exec "${CONTAINER}" bash -c '
  export DEBIAN_FRONTEND=noninteractive
  apt-get update -qq
  apt-get install -y -qq samba samba-common-bin krb5-user winbind smbclient ldb-tools
'

if docker exec "${CONTAINER}" test -f /var/lib/samba/private/sam.ldb; then
  echo "==> Domain already provisioned, skipping"
else
  echo "==> Provisioning PSYNCOPATE.COM"
  # --dns-backend=SAMBA_INTERNAL: required, not optional - confirmed live
  # that --dns-backend=NONE leaves Samba's CLDAP netlogon responder unable
  # to answer (it needs the DNS zone's site/domain-GUID data), which makes
  # both `net ads join` and sssd's ad provider fail discovery even on a
  # fully-native, full-UDP network with no tunnel involved at all.
  #
  # --option="vfs objects=...xattr_tdb": provisioning otherwise fails
  # setting the sysvol NT ACL - writing the security.NTACL xattr needs
  # CAP_SYS_ADMIN, which a default `docker run` container doesn't have.
  # xattr_tdb emulates the same ACL storage in a Samba-managed tdb file
  # instead of real filesystem xattrs, sidestepping the capability
  # requirement - Samba's own documented workaround for this class of
  # restricted environment.
  docker exec "${CONTAINER}" samba-tool domain provision \
    --use-rfc2307 \
    --realm=PSYNCOPATE.COM \
    --domain=PSYNCOPATE \
    --server-role=dc \
    --dns-backend=SAMBA_INTERNAL \
    --host-name=SAMBADC1 \
    --option="vfs objects = dfs_samba4 acl_xattr xattr_tdb" \
    --adminpass="${ADMIN_PASSWORD}"
fi

echo "==> Writing krb5.conf (explicit, non-DNS - matches this repo's usual pattern)"
docker exec "${CONTAINER}" bash -c '
cat > /etc/krb5.conf <<EOF
[libdefaults]
    default_realm = PSYNCOPATE.COM
    dns_lookup_realm = false
    dns_lookup_kdc = false
    udp_preference_limit = 1

[realms]
    PSYNCOPATE.COM = {
        kdc = 127.0.0.1:88
        admin_server = 127.0.0.1:464
        default_domain = psyncopate.com
    }

[domain_realm]
    .psyncopate.com = PSYNCOPATE.COM
    psyncopate.com = PSYNCOPATE.COM
EOF
'

if ! docker exec "${CONTAINER}" pgrep -f "samba -i" >/dev/null 2>&1; then
  echo "==> Starting samba (AD DC mode)"
  docker exec -d "${CONTAINER}" bash -c 'nohup samba -i --debug-stdout > /var/log/samba-ad.log 2>&1 &'
  sleep 8
fi

echo "==> Verifying"
docker exec "${CONTAINER}" bash -c "echo '${ADMIN_PASSWORD}' | kinit administrator@PSYNCOPATE.COM && klist"
echo "==> Samba AD DC ready. Next: ./scripts/kerberos/02-create-principals.sh"
