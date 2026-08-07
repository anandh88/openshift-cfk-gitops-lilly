#!/usr/bin/env bash
# Opens the SSH reverse tunnel that lets Kafka Connect (inside the
# cluster) reach the Docker Desktop AD DC's Kerberos port, and adds the
# node-side iptables rule that makes it actually work with Java's
# Kerberos client. Both are local dev/validation harness steps, not
# committed infrastructure - re-run after every `crc stop`/`crc start` or
# Mac reboot, since neither survives one.
#
# Why the iptables rule: ssh -R only carries TCP, but Java's krb5 client
# still attempts a UDP send first regardless of udp_preference_limit
# (confirmed live - a real JDK quirk/bug, not a config mistake). Sending
# UDP to a port nothing is listening on gets an immediate ICMP "port
# unreachable" from the node itself, which Java treats as fatal rather
# than falling back to TCP. Dropping (not rejecting) that UDP traffic
# makes Java's send silently time out instead, which *does* trigger its
# normal TCP fallback.
set -euo pipefail

NODE_PORT="${NODE_PORT:-18088}"
SAMBA_LOCAL_PORT="${SAMBA_LOCAL_PORT:-8088}"

echo "==> Adding node-side iptables DROP rule for UDP ${NODE_PORT} (idempotent-ish; ignore 'already exists')"
oc debug node/crc -- chroot /host bash -c "
  iptables -C INPUT -p udp --dport ${NODE_PORT} -j DROP 2>/dev/null || iptables -I INPUT -p udp --dport ${NODE_PORT} -j DROP
" 2>&1 | grep -v "^Starting pod\|^Removing debug pod\|^To use host binaries"

if pgrep -f "ssh.*-R 0.0.0.0:${NODE_PORT}:localhost:${SAMBA_LOCAL_PORT}" >/dev/null 2>&1; then
  echo "==> Tunnel already running"
else
  echo "==> Opening reverse tunnel: node:${NODE_PORT} -> Docker Desktop sambaad:88"
  ssh -i ~/.crc/machines/crc/id_ed25519 -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -p 2222 -N \
    -R "0.0.0.0:${NODE_PORT}:localhost:${SAMBA_LOCAL_PORT}" core@127.0.0.1 &
  disown
  sleep 2
fi

echo "==> Verifying from a pod (needs SQLSERVER_HOST to already be set up - any pod works)"
NODE_IP="$(oc get nodes -o jsonpath='{.items[0].status.addresses[?(@.type=="InternalIP")].address}')"
echo "    Node internal IP: ${NODE_IP}"
echo "    connect-krb5-conf's [realms] kdc/admin_server should be ${NODE_IP}:${NODE_PORT}"
echo "==> Done."
