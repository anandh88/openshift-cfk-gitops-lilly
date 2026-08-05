#!/usr/bin/env bash
# Fully tears down the platform in reverse dependency order: Argo CD
# Applications first (so their managed resources are pruned cleanly),
# then namespaces, then CRDs, then the helm-installed cluster add-ons.
# Destructive and irreversible — requires an explicit "yes" to proceed.
set -euo pipefail

echo "This will DELETE the entire openshift-cfk-gitops platform from the cluster."
read -r -p "Type 'yes' to continue: " confirmation
if [[ "${confirmation}" != "yes" ]]; then
  echo "Aborted."
  exit 1
fi

echo "==> Deleting Argo CD Applications (reverse sync-wave order)"
oc delete application flink-jobs -n argocd --ignore-not-found
oc delete application confluent-platform -n argocd --ignore-not-found
oc delete application cmf-operator -n argocd --ignore-not-found
oc delete application confluent-operator -n argocd --ignore-not-found
oc delete application platform-root -n argocd --ignore-not-found

echo "==> Waiting for finalizers to release managed resources"
sleep 15

echo "==> Deleting platform namespaces"
oc delete namespace flink-jobs --ignore-not-found
oc delete namespace cmf-operator --ignore-not-found
oc delete namespace confluent --ignore-not-found
oc delete namespace confluent-operator --ignore-not-found
oc delete namespace argocd --ignore-not-found

echo "==> Deleting CFK and CMF CRDs"
oc get crd -o name | grep -E 'platform\.confluent\.io|cmf\.confluent\.io' | xargs -r oc delete

echo "==> Uninstalling helm releases"
helm uninstall sealed-secrets -n kube-system || true
helm uninstall cert-manager -n cert-manager || true

echo "==> Deleting remaining support namespaces"
oc delete namespace cert-manager --ignore-not-found

echo "==> Teardown complete"
