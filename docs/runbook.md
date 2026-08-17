# Operations Runbook

## Deploy a new Flink job

1. Add/update the job's source under `flink-jobs/<job-name>/` (Maven project).
2. Push to `main` with changes under `flink-jobs/**` — `ci-flink-build.yaml`
   builds the jar, builds/pushes the image to `ghcr.io/anandh88/flink-jobs/<job-name>`,
   and auto-commits the new tag into `base/flink-jobs/flink-application.yaml`.
3. If it's a brand-new job (not an update to `claims-processor`), copy
   `base/flink-jobs/flink-application.yaml` to a new file, update `metadata.name`,
   `jobSpec.jarURI`/`entryClass`/`args`, and add it to
   `base/flink-jobs/kustomization.yaml`.
4. Confirm `flink-jobs` Application syncs: `argocd app get flink-jobs`.
5. Watch the job come up: `oc get flinkapplication -n flink-jobs -w`.

## Update Kafka config via GitOps

1. Edit `configOverrides` in `base/confluent-platform/kafka.yaml` (cluster-wide
   defaults) or the relevant overlay patch (`overlays/local/kafka-patch.yaml`
   / `overlays/prod/kafka-patch.yaml`) for environment-specific values.
2. Open a PR — `ci-validate.yaml` will build the kustomization to catch typos.
3. Merge to `main`. Argo CD's `confluent-platform` app auto-syncs (prune+selfHeal).
4. Verify: `oc get kafka kafka -n confluent -o yaml | yq '.spec.configOverrides'`.

## Rotate TLS certificates

1. Certificates auto-renew via cert-manager at `renewBefore` (720h before
   expiry) — no action needed in the common case.
2. To force an immediate rotation (e.g. suspected key compromise):
   ```
   oc delete secret kafka-tls-secret -n confluent
   oc annotate certificate kafka-tls -n confluent cert-manager.io/issue-temporary-certificate- --overwrite
   ```
   cert-manager will reissue automatically from `platform-ca-issuer`.
3. Rolling-restart the affected component so it picks up the new secret:
   `oc rollout restart statefulset/kafka -n confluent`.
4. To rotate the root CA itself, delete `platform-root-ca-secret` in
   `cert-manager` namespace — every downstream cert re-issues off the new CA.

## Unseal / re-seal secrets after cluster recreation

The sealed-secrets controller's private key is unique per cluster install —
sealed values from a previous cluster **will not decrypt** on a new one.

1. After `scripts/bootstrap.sh` reinstalls sealed-secrets, fetch the new
   public cert (done automatically by the script into
   `/tmp/sealed-secrets-public-cert.pem`).
2. Re-run `scripts/seal-secrets.sh` to regenerate every SealedSecret manifest
   under `base/confluent-platform/secrets/` and `base/flink-jobs/`.
3. Commit and push the regenerated files — Argo CD will apply them and the
   controller will decrypt into real `Secret` objects in-cluster.
4. If you need secrets to survive cluster recreation, back up the
   controller's signing key (see docs/troubleshooting.md, "Sealed secret not
   decrypting") **before** tearing down.

## Promote from local overlay to prod overlay

1. Confirm `overlays/prod/kafka-patch.yaml` and `overlays/prod/flink-patch.yaml`
   reflect the desired production sizing (replicas, resources, replication
   factors) — these are intentionally more conservative than `overlays/local`.
2. Point the `confluent-platform` Argo CD Application's `spec.source.path`
   at `overlays/prod` instead of `overlays/local` (either directly, or via a
   separate prod `Application`/`ApplicationSet` pointed at a prod cluster).
3. Because `default.replication.factor` and `min.insync.replicas` change
   between overlays, promoting an existing cluster in place requires a
   rolling reconfiguration, not just a patch — plan a maintenance window.
4. Re-run `scripts/seal-secrets.sh` against the prod cluster's own
   sealed-secrets controller; prod secrets must never reuse local values.

## Approve a Manual installPlan for CFK upgrades

`base/confluent-operator/subscription.yaml` sets `installPlanApproval: Manual`
so operator upgrades are never silently applied by Argo CD.

1. Check for a pending install plan:
   ```
   oc get installplan -n confluent-operator
   ```
2. Review what the new plan changes:
   ```
   oc get installplan <name> -n confluent-operator -o yaml
   ```
3. Approve it:
   ```
   oc patch installplan <name> -n confluent-operator \
     --type merge --patch '{"spec":{"approved":true}}'
   ```
4. Watch the operator roll out: `oc get csv -n confluent-operator -w`.
