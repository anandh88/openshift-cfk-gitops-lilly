#!/usr/bin/env bash
# Runs 01-init-kdc.sh through 05-deploy-connector.sh in order, then
# validate-kerberos.sh. Individual scripts still exist and are still the
# right tool for a partial re-run (e.g. docs/kerberos-runbook.md's keytab
# rotation flow only needs 02-04) - this is just the convenience path for
# a first-time, start-to-finish run.
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
echo "    04-seal-keytabs.sh only writes local files - commit and push them:"
echo "      git add base/confluent-platform/secrets/connect-keytab-sealed.yaml base/sqlserver/sqlserver-keytab-sealed.yaml"
echo "      git commit -m 'seal real Kerberos keytabs for connect and sqlserver'"
echo "      git push"
echo "    Or, to see the real keytabs take effect immediately on this cluster without"
echo "    waiting for Argo CD's next sync, apply them directly:"
echo "      oc apply -f base/confluent-platform/secrets/connect-keytab-sealed.yaml"
echo "      oc apply -f base/sqlserver/sqlserver-keytab-sealed.yaml"
