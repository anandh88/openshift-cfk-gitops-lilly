#!/usr/bin/env bash
# Runs 01-init-kdc.sh through 05-deploy-connector.sh in order, then
# validate-kerberos.sh. Individual scripts still exist and are still the
# right tool for a partial re-run (e.g. docs/kerberos-runbook.md's keytab
# rotation flow only needs 02-04) - this is just the convenience path for
# a first-time, start-to-finish run.
#
# Requires SQLSERVER_HOST (and optionally SQLSERVER_PORT, default 1433) -
# SQL Server runs outside this cluster (see docs/kerberos-architecture.md),
# so its address is environment-specific. 02/03/05 all fail fast with a
# clear error if unset.
#
# Stops on the first failure (set -e) rather than continuing into a step
# whose prerequisites didn't succeed.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

steps=(
  "01-init-kdc.sh"
  "02-create-principals.sh"
  "03-export-keytabs.sh"
  "04-seal-keytabs.sh"
  "05-deploy-connector.sh"
)

for step in "${steps[@]}"; do
  echo "############################################################"
  echo "# ${step}"
  echo "############################################################"
  "${SCRIPT_DIR}/${step}"
  echo
done

echo "############################################################"
echo "# validate-kerberos.sh"
echo "############################################################"
"${SCRIPT_DIR}/validate-kerberos.sh"

echo
echo "==> All steps complete."
echo "    04-seal-keytabs.sh only writes a local file - commit and push it:"
echo "      git add base/confluent-platform/secrets/connect-keytab-sealed.yaml"
echo "      git commit -m 'seal real Kerberos keytab for connect'"
echo "      git push"
echo "    Or, to see the real keytab take effect immediately on this cluster without"
echo "    waiting for Argo CD's next sync, apply it directly:"
echo "      oc apply -f base/confluent-platform/secrets/connect-keytab-sealed.yaml"
