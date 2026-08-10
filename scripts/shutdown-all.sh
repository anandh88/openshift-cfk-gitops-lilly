#!/usr/bin/env bash
# Idempotent clean shutdown for the whole local CFK + SQL Server + Kerberos
# stack (the counterpart to scripts/bootstrap-all.sh). Safe to re-run any
# time, including when some or all of it is already stopped - every step
# checks current state before acting instead of erroring out.
#
# Frees: the CRC VM's reserved CPU/memory, any reverse SSH tunnels left
# running on the Mac, and the Samba AD DC / SQL Server Docker containers.
# Does NOT delete the CRC VM itself, its cache/image data, or Docker
# volumes (sambaad-data, PVCs, etc.) - use `crc delete` / `docker volume rm`
# / `docker system prune` separately for a full teardown, since those are
# destructive and not implied by a routine "shut down for the day".
set -uo pipefail

SQLSERVER_CONTAINER="${SQLSERVER_CONTAINER:-sqltest2}"
SAMBA_CONTAINER="${SAMBA_CONTAINER:-sambaad}"

pass() { echo "  OK: $1"; }
info() { echo "  ..: $1"; }
step() { echo; echo "############################################################"; echo "# $1"; echo "############################################################"; }

step "1. Reverse SSH tunnels (KDC + SQL Server)"
TUNNEL_PIDS="$(pgrep -f "ssh.*-R 0.0.0.0:(14330|18088)" 2>/dev/null || true)"
if [[ -n "${TUNNEL_PIDS}" ]]; then
  # shellcheck disable=SC2086
  kill ${TUNNEL_PIDS} 2>/dev/null || true
  pass "stopped tunnel process(es): ${TUNNEL_PIDS}"
else
  pass "no tunnel processes running"
fi

step "2. Samba AD DC / SQL Server containers (Docker Desktop)"
for c in "${SAMBA_CONTAINER}" "${SQLSERVER_CONTAINER}"; do
  if ! docker inspect "${c}" >/dev/null 2>&1; then
    pass "${c}: does not exist, nothing to stop"
  elif [[ "$(docker inspect -f '{{.State.Status}}' "${c}" 2>/dev/null)" != "running" ]]; then
    pass "${c}: already stopped"
  else
    docker stop "${c}" >/dev/null && pass "${c}: stopped" || echo "  FAIL: could not stop ${c}"
  fi
done

step "3. CRC OpenShift VM"
CRC_STATE="$(crc status 2>&1 || true)"
if echo "${CRC_STATE}" | grep -q "CRC VM:.*Running"; then
  info "stopping CRC VM (this can take a minute)"
  crc stop -f >/dev/null 2>&1 && pass "CRC VM stopped" || echo "  FAIL: crc stop reported an error - check 'crc status'"
elif echo "${CRC_STATE}" | grep -qi "not seem to be setup correctly\|is not running\|machine.*stopped"; then
  pass "CRC VM already stopped (or in a stopped/unreachable state)"
else
  info "unrecognized crc status output, attempting stop anyway:"
  echo "${CRC_STATE}"
  crc stop -f >/dev/null 2>&1 || true
fi

step "4. Orphaned crc daemon process"
# `crc stop` can leave the top-level `crc daemon` management process (not
# the VM itself) running - confirmed live that after a VM crash it stays
# alive reporting stale state until killed and restarted by the next
# `crc start`/`crc status` call.
DAEMON_PID="$(pgrep -f '/crc daemon' 2>/dev/null || true)"
if [[ -n "${DAEMON_PID}" ]]; then
  kill "${DAEMON_PID}" 2>/dev/null && pass "stopped orphaned crc daemon (pid ${DAEMON_PID})"
else
  pass "no crc daemon process running"
fi

step "Summary"
echo "  CRC VM, Docker containers (${SAMBA_CONTAINER}, ${SQLSERVER_CONTAINER}), and tunnels are stopped."
echo "  Disk data (CRC cache, Docker volumes/images) is left in place - not a full teardown."
echo "  Re-run ./scripts/bootstrap-all.sh to bring everything back up."
