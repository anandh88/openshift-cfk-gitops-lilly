# OpenShift Confluent Platform GitOps - In-Depth Workspace Analysis

## Executive Summary

This is a **production-grade GitOps repository** that automates the deployment and management of a complete Confluent Kafka platform ecosystem on Red Hat OpenShift. It uses ArgoCD as the GitOps controller to maintain declarative state across operators, Kafka infrastructure, schema registries, connectors, and Apache Flink applications.

---

## 1. Project Intent & Architecture

### 1.1 Core Purpose
The repository enables **Infrastructure-as-Code (IaC)** management of a Confluent Platform deployment through Git as the single source of truth. Changes are automatically reconciled to the cluster via ArgoCD's continuous deployment model.

### 1.2 Technology Stack

| Component | Version | Purpose |
|-----------|---------|---------|
| **Kubernetes** | OpenShift (CRC) | Container orchestration platform |
| **GitOps Controller** | ArgoCD v2.10.0 | Continuous deployment & reconciliation |
| **Confluent Platform** | 8.2.2 | Apache Kafka 4.2 event streaming |
| **CFK Operator** | 3.3.0 | Kubernetes-native Kafka operator |
| **CMF Operator** | Latest | Confluent Manager for Apache Flink |
| **Certificate Management** | cert-manager v1.14.0 | TLS/mTLS certificate lifecycle |
| **Secrets Management** | Sealed Secrets v2.15.0 | Encrypted secrets in Git |

### 1.3 Architecture Diagram

```
        ┌──────────────────────────┐
        │   Git Repository (main)   │
        └────────────┬─────────────┘
                     │
                     ▼
        ┌──────────────────────────┐
        │  ArgoCD (platform-root)   │ ← App-of-Apps pattern
        │  Namespace: argocd        │
        └────────────┬─────────────┘
                     │ Manages
         ┌───────────┼───────────┐
         │           │           │
         ▼           ▼           ▼
    ┌─────────┐ ┌──────────┐ ┌────────────┐
    │Operator │ │Confluent │ │Flink Jobs  │
    │App      │ │Platform  │ │App         │
    │(Wave 0) │ │App (Wave │ │(Wave 2)    │
    │         │ │1)        │ │            │
    └────┬────┘ └────┬─────┘ └────┬───────┘
         │           │            │
         ▼           ▼            ▼
    ┌─────────┐ ┌──────────────┐ ┌────────────┐
    │CFK Ops  │ │Kafka (KRaft) │ │Flink Jobs  │
    │CMF Ops  │ │Schema Reg    │ │            │
    │         │ │Connect       │ │            │
    │         │ │Control Ctr   │ │            │
    └─────────┘ └──────────────┘ └────────────┘
```

### 1.4 Namespace & Resource Distribution

The deployment spans **6 namespaces** with clear separation of concerns:

| Namespace | Wave | Components | Purpose |
|-----------|------|-----------|---------|
| `argocd` | Bootstrap | Argo CD server/repo-server/dex | GitOps control plane |
| `cert-manager` | Bootstrap | Cert-manager pods, webhooks | TLS certificate lifecycle |
| `confluent-operator` | Wave 0 | CFK operator, Subscription | Kafka operator deployment |
| `cmf-operator` | Wave 0 | CMF Helm release | Flink operator deployment |
| `confluent` | Waves 1-5 | KRaftController, Kafka, SR, Connect, C3, RestProxy | Kafka platform stack |
| `flink-jobs` | Wave 2 | FlinkEnvironment, FlinkApplication | Stream processing workloads |

---

## 2. Component & Dependency Analysis

### 2.1 Kafka Platform Stack (Confluent Namespace)

**Wave 1 - Pre-requisites:**
- **KRaftController** (3 replicas) - Kafka Raft consensus layer, no external Zookeeper needed
- **FlinkEnvironment** - Flink runtime environment

**Wave 2 - Core Brokers:**
- **Kafka** (3 replicas) - Message brokers, SASL/TLS authentication, PLAIN/SCRAM support
- **FlinkApplication** (claims-processor) - Stream processing job using Flink
- **flink-jobs** app - Managed by CMF operator

**Wave 3 - Schema Management:**
- **SchemaRegistry** - Avro/Protobuf schema storage, depends on Kafka for backend

**Wave 4 - Integration:**
- **Connect** - JDBC connector for SQL Server ingestion (Kerberos-enabled)
- **KafkaRestProxy** - HTTP API to Kafka

**Wave 5 - Monitoring & Management:**
- **ControlCenter** - Confluent's enterprise UI for monitoring, alerting (with Prometheus/Alertmanager sidecars)
- **ControlCenter Next Gen** v2.5.0 (independently versioned from platform since CP 8.0)

### 2.2 Communication & Data Flow

```
┌─────────────────────────────────────────────┐
│         Component Communication Map         │
├──────────────┬────────────────┬─────────────┤
│ Source       │ Destination    │ Port/Auth   │
├──────────────┼────────────────┼─────────────┤
│ KRaftCtrl    │ Kafka          │ 9093 TLS    │
│ Kafka        │ Kafka (internal)│ 9092 SASL  │
│ Kafka        │ External       │ 9094 SASL   │
│ SchemaReg    │ Kafka          │ 9092 SASL   │
│ Connect      │ Kafka/SR       │ 9092/8081   │
│ ControlCtr   │ All components │ TLS + Auth  │
│ Flink        │ Kafka          │ 9092 SASL   │
└──────────────┴────────────────┴─────────────┘
```

**Authentication Types:**
- **Internal (pod-to-pod)**: mTLS + SASL/PLAIN
- **External (client access)**: SASL/SCRAM/PLAIN + TLS
- **Kerberos**: Connect → SQL Server (via Samba AD DC external integration)

---

## 3. TLS/Certificate Architecture

### 3.1 Certificate Hierarchy

```
┌─────────────────────────────────────┐
│ selfsigned-bootstrap ClusterIssuer   │
│ (mints root CA)                     │
└────────────────┬────────────────────┘
                 │ Creates
         ┌───────▼────────┐
         │ platform-root- │
         │ ca Certificate │
         └───────┬────────┘
                 │ Signed by
         ┌───────▼────────────────────┐
         │ platform-ca-issuer         │
         │ ClusterIssuer              │
         └───────┬────────────────────┘
                 │ Issues all leaf certs
    ┌────────┬───────────┬────────┬─────────┬──────────┐
    │        │           │        │         │          │
    ▼        ▼           ▼        ▼         ▼          ▼
 kafka-   schema-    connect-  control-  restproxy- argocd-
 tls      registry-  tls       center-   tls        server-
 secret   tls        secret    tls                  tls
          secret     secret    secret

Every leaf cert valid for both internal (DNS SAN) and external (Route SAN)
```

### 3.2 Certificate Management

- **Auto-renewal**: 720 hours before expiry (cert-manager's default)
- **Root CA**: Selfsigned, stored in `platform-root-ca-secret` (cert-manager namespace)
- **Leaf Certs**: Issued by `platform-ca-issuer`, auto-renewed
- **Disaster Recovery**: Private keys can be backed up before cluster recreation
- **Force Rotation**: Delete specific certificate Secret + annotate Certificate CR

### 3.3 Multi-SAN Strategy

Each certificate covers:
- **Internal SAN**: In-cluster pod DNS (e.g., `kafka.confluent.svc.cluster.local`)
- **External SAN**: OpenShift Route DNS (e.g., `kafka.apps-crc.testing`) or wildcard (`*.apps-crc.testing`)

---

## 4. Secrets Management & Security

### 4.1 Sealed Secrets Pattern

**Why:** Raw `Secret` objects in Git are a security risk. SealedSecret CRDs encrypt values asymmetrically per cluster.

**Flow:**
```
1. Human creates Secret in plaintext (local, never committed)
   └─ kubeseal tool with --fetch-cert
2. Public key encrypts the Secret → SealedSecret manifest (safe to commit)
3. Sealed-secrets controller in cluster has private key
4. Controller decrypts SealedSecret → real Secret (in-memory only)
5. Application pods mount the decrypted Secret
```

### 4.2 Cluster Lifecycle: Sealed Secrets Re-sealing

When cluster is recreated:
1. sealed-secrets controller gets a **new private key** → old SealedSecrets won't decrypt
2. Fetch the new **public certificate** from the fresh controller
3. Run `scripts/seal-secrets.sh` to regenerate all SealedSecret manifests
4. Commit & push regenerated files
5. ArgoCD applies → controller decrypts with new key

**Files requiring this process:**
- `base/confluent-platform/secrets/*.yaml` (Kafka SASL, C3 creds, Connect keytab)
- `base/flink-jobs/flink-kafka-sasl-sealed.yaml`

### 4.3 Secrets in Repo

**Currently Sealed:**
- Kafka SASL credentials (internal + external listeners)
- C3 admin credentials
- Connect Kerberos keytab
- Flink Kafka SASL credentials

**Scanning:**
- `ci-validate.yaml` scans for plaintext `kind: Secret` objects
- trufflehog detects accidentally-committed API keys/tokens
- .gitignore blocks `*.pem`, `*.key`, `sealed-secrets-master-key*.yaml`

---

## 5. CI/CD Pipeline in Detail

### 5.1 Three-Stage Automation

```
Push to branch
       │
       ├─────────────────────────────────────┐
       │                                     │
       ▼                                     ▼
[ci-validate.yaml]                   [ci-flink-build.yaml]
  (All PRs & pushes)                  (Flink changes only)
  - Lint YAML                         - Maven build
  - Kustomize build                   - Docker build/push
  - Secret scan                       - Auto-commit image tag
  - trufflehog
  - App CR validation

       All checks pass
       │
       ▼
  Merge to main
       │
       ▼
[cd-argocd-sync.yaml]
  (Self-hosted runner)
  - Sync operators (wave 0)
  - Sync platform (wave 1)
  - Sync flink-jobs (wave 2)
  - Verify health
```

### 5.2 ci-validate.yaml (Pull Requests & Pushes)

**Triggers:** Every PR and push to any branch

**Jobs:**

1. **lint** - yamllint with custom config
   - Max line length: 200 chars (warning)
   - Disabled: document-start, truthy checks
   - Runs: ~1 min

2. **kustomize-build** - Validates all overlays
   - Builds `overlays/local` (active development)
   - Builds `base/confluent-platform` (base config)
   - Builds `base/flink-jobs` (Flink configuration)
   - Catches typos, CRD mismatches, missing resources
   - Runs: ~2 min

3. **secret-scan** - Prevents credentials leakage
   - Fails if any `kind: Secret` (not SealedSecret) found
   - Runs trufflehog for actual secret detection (PII, tokens, etc.)
   - Runs: ~1 min

4. **argocd-app-validate** - Python script validating every Application CR
   - Checks `spec.syncPolicy` exists
   - Checks `spec.source.repoURL` populated
   - Checks `spec.destination.server` populated
   - Runs: ~30 sec

**Total Pipeline Time:** ~5-7 minutes

### 5.3 ci-flink-build.yaml (Flink Image Pipeline)

**Triggers:** Pushes to `main` with changes under `flink-jobs/**`

**Steps:**

1. **Maven build** - `flink-jobs/claims-processor/pom.xml`
   - Compiles claims-processor jar
   - Skips tests (`-DskipTests`)
   - Output: `target/claims-processor-1.0.jar`

2. **Docker login** - To ghcr.io (GitHub Container Registry)
   - Uses `GITHUB_TOKEN` secret (automatic)
   - Username: `${{ github.actor }}`

3. **Image metadata** - Via `docker/metadata-action`
   - Tags: Short SHA, branch name, semver
   - Example: `ghcr.io/mkurre/flink-jobs/claims-processor:sha-a1b2c3d`

4. **Build & push** - Multi-platform aware
   - Context: `flink-jobs/claims-processor` (Dockerfile there)
   - Push only on main (not PRs)
   - Skipped on PR (dry-run build)

5. **Auto-commit image tag** - Using `yq`
   - Updates `base/flink-jobs/flink-application.yaml`
   - Field: `.spec.image = "ghcr.io/.../claims-processor:sha-<SHORT_SHA>"`
   - Commit message: "chore: update flink image [skip ci]" (prevents re-trigger)
   - Pushes back to main

**Note:** Git push from workflow → ArgoCD detects new image ref → auto-syncs Flink app

### 5.4 cd-argocd-sync.yaml (Manual Deployment)

**Triggers:** 
- Pushes to `main` (automatic)
- Workflow dispatch (manual button in GitHub UI)

**Special:** Runs on **self-hosted runner** (not GitHub-hosted)

**Reason:** CRC cluster exists only on developer's Mac; *.apps-crc.testing DNS only resolvable locally

**Steps:**

1. **ArgoCD CLI setup** - Version-matched to server (v2.10.0)
   - Downloaded to `$RUNNER_TEMP/argocd-bin`
   - Client/server gRPC version must match exactly
   - Installed job-scoped (doesn't conflict with Mac's Homebrew install)

2. **ArgoCD login**
   - Server: `${{ secrets.ARGOCD_SERVER }}`
   - Username/password: GitHub secrets
   - Uses `--insecure` (self-signed certs) + `--grpc-web` (HTTP proxy)

3. **Sync workflow (with health checks)**
   ```
   Loop:
   1. confluent-operator (wave 0)
      - argocd app sync confluent-operator --async
      - argocd app wait confluent-operator --health --timeout 300s
   
   2. confluent-platform (wave 1)
      - argocd app sync confluent-platform --async
      - argocd app wait confluent-platform --health --timeout 600s
   
   3. flink-jobs (wave 2)
      - argocd app sync flink-jobs --async
      (no wait - informational)
   
   4. App list (always, even on failure)
      - argocd app list
   ```

4. **Async sync pattern**
   - `--async` starts sync in background immediately
   - `--wait` blocks until Application reaches desired state
   - Timeouts: 300s (operators), 600s (platform), none (flink)

---

## 6. Configuration Management & Kustomization

### 6.1 Directory Structure (Kustomize Composition)

```
base/
├── confluent-operator/
│   ├── kustomization.yaml (OLM Subscription + OperatorGroup)
│   ├── subscription.yaml (Manual approval for CFK upgrades)
│   └── operatorgroup.yaml
├── confluent-platform/
│   ├── kustomization.yaml
│   ├── kraft.yaml (3 KRaftController replicas, PLAIN auth)
│   ├── kafka.yaml (3 Kafka brokers, mTLS + SASL/PLAIN)
│   ├── schemaregistry.yaml (Schema registry with C3 integration)
│   ├── connect.yaml (JDBC source connector for SQL Server)
│   ├── restproxy.yaml (HTTP API to Kafka)
│   ├── controlcenter.yaml (UI + Prometheus/Alertmanager sidecars)
│   ├── connect-kerberos-patch.yaml (Kerberos keytab mounts)
│   ├── connect-krb5-configmap.yaml (krb5.conf)
│   ├── connect-jaas-configmap.yaml (JAAS config for SPNEGO)
│   ├── connect-truststore-configmap.yaml (Kerberos truststore)
│   ├── sqlserver-claims-topic.yaml (Auto-create topic for SQL ingestion)
│   ├── secrets/
│   │   ├── kafka-sasl-sealed.yaml (Internal broker SASL)
│   │   ├── kafka-external-sasl-sealed.yaml (External client SASL)
│   │   ├── c3-credentials-sealed.yaml (ControlCenter admin)
│   │   └── connect-keytab-sealed.yaml (Kerberos keytab)
│   └── patches/ (applied by overlays)
├── flink-jobs/
│   ├── kustomization.yaml
│   ├── flink-environment.yaml (FlinkEnvironment, namespace + cluster config)
│   ├── flink-application.yaml (FlinkApplication for claims-processor)
│   ├── cmf-restclass.yaml (REST class for Flink API)
│   ├── flink-rbac.yaml (Service accounts, roles, bindings)
│   ├── flink-kafka-sasl-sealed.yaml (Kafka credentials for Flink)
│   └── kustomization.yaml

overlays/
├── local/
│   ├── kustomization.yaml
│   │   - bases: ../../base/confluent-platform
│   │   - patches: kraft, kafka, sr, connect
│   │   - commonAnnotations: deployment-environment=local-crc
│   ├── kraft-patch.yaml (1 replica, minimal resources)
│   ├── kafka-patch.yaml (1 broker, 1 replication, auto.create.topics)
│   ├── sr-patch.yaml (1 replica)
│   └── connect-patch.yaml (1 replica, Kerberos enabled)
└── prod/
    ├── kustomization.yaml
    ├── kafka-patch.yaml (3 brokers, min.insync.replicas: 2)
    └── (sr-patch, connect-patch likely similar to local but scaled)
```

### 6.2 Kustomization Pattern: Patches vs. Resources

**base/confluent-platform/kustomization.yaml:**
```yaml
resources:
  - secrets/kafka-sasl-sealed.yaml
  - kraft.yaml
  - kafka.yaml
  - schemaregistry.yaml
  - connect.yaml
  - ...
patches:
  - path: connect-kerberos-patch.yaml
    # Adds Kerberos volumes/env to Connect without editing connect.yaml
```

**overlays/local/kustomization.yaml:**
```yaml
bases:
  - ../../base/confluent-platform
patches:
  - path: kraft-patch.yaml
  - path: kafka-patch.yaml
  - path: sr-patch.yaml
  - path: connect-patch.yaml
images:
  - name: confluentinc/cp-server
    newTag: "8.2.2"
  - name: confluentinc/confluent-init-container
    newTag: "3.3.0"
commonAnnotations:
  deployment-environment: local-crc
```

### 6.3 How Overlays Work

1. **overlays/local/kustomization.yaml** points to `bases: ../../base/confluent-platform`
2. **base/confluent-platform/kustomization.yaml** lists all resources (Kafka, SR, etc.)
3. **overlays/local** applies patches (kraft-patch.yaml, kafka-patch.yaml, etc.)
4. **Kustomize merges** all YAML manifests, applies patches, sets image tags, adds annotations

**Result:** Full deployed manifest with local overlay config applied

### 6.4 Environment Promotion Workflow

**Development (overlays/local):**
- 1 KRaftController replica
- 1 Kafka broker
- `default.replication.factor: 1`
- `min.insync.replicas: 1`
- `auto.create.topics.enable: true`
- Minimal resource requests

**Promotion to Production (overlays/prod):**
1. Edit `overlays/prod/kustomization.yaml` → point source.path to `overlays/prod`
2. **OR** create separate prod Application CR pointing to overlays/prod
3. Plan rolling reconfiguration (replication factor changes require Kafka restart)
4. **Re-seal all secrets** against prod cluster's sealed-secrets key
5. Merge to main → ArgoCD syncs to prod cluster

---

## 7. Deployment Bootstrap Process

### 7.1 scripts/bootstrap.sh - Complete Sequence

```bash
Step 1:  oc whoami
         └─ Verify cluster login

Step 2:  oc apply -f base/namespaces/all-namespaces.yaml
         └─ Create: argocd, cert-manager, confluent-operator, confluent, cmf-operator, flink-jobs

Step 3:  helm install cert-manager (jetstack, v1.14.0)
         └─ Full CRDs: Certificate, ClusterIssuer, Issuer, CertificateRequest

Step 4:  oc rollout status deployment/cert-manager -n cert-manager
         └─ Wait for webhooks ready (critical: webhook must be live before issuing certs)

Step 5:  oc apply -f bootstrap/cert-manager-issuers.yaml
         ├─ ClusterIssuer: selfsigned-bootstrap
         ├─ Certificate: platform-root-ca (self-signed, mints root CA)
         └─ ClusterIssuer: platform-ca-issuer (signed by platform-root-ca)

Step 6:  oc apply -f bootstrap/platform-certificates.yaml
         └─ Issue all leaf certs:
            - kafka-tls, schemaregistry-tls, connect-tls, controlcenter-tls, restproxy-tls, argocd-server-tls

Step 7:  helm install sealed-secrets (bitnami, v2.15.0)
         └─ Single pod in kube-system, encrypts/decrypts SealedSecret CRDs

Step 8:  kubeseal --fetch-cert > /tmp/sealed-secrets-public-cert.pem
         └─ Save public key for local secret sealing (seals plaintext Secrets)

Step 9:  [Manual] Run scripts/seal-secrets.sh
         └─ Uses public cert to seal Kafka SASL, C3 creds, Connect keytab, etc.

Step 10: oc apply -f bootstrap/platform-rbac.yaml
         └─ RBAC: ServiceAccounts, Roles, RoleBindings for namespace isolation

Step 11: oc apply -f bootstrap/network-policies.yaml
         └─ Default-deny network policies, then allow specific traffic

Step 12: oc apply -f bootstrap/argocd-install.yaml
         └─ Create argocd namespace (pre-req before official install manifest)

Step 13: oc apply -n argocd -f <upstream manifest>
         └─ ArgoCD core install (non-HA single-pod repo-server)
```

**Total Time:** ~5-10 minutes (mostly Helm chart pulls + pod rollouts)

### 7.2 Post-Bootstrap: Secrets Sealing

**scripts/seal-secrets.sh** must be run after bootstrap to encrypt sensitive data:

```bash
# Generates/seals all Secrets into SealedSecret CRDs
kubeseal -f plaintext-secret.yaml > sealed-secret.yaml

# Locations sealed:
- base/confluent-platform/secrets/kafka-sasl-sealed.yaml
- base/confluent-platform/secrets/kafka-external-sasl-sealed.yaml
- base/confluent-platform/secrets/c3-credentials-sealed.yaml
- base/confluent-platform/secrets/connect-keytab-sealed.yaml
- base/flink-jobs/flink-kafka-sasl-sealed.yaml
```

**Note:** Must be re-run after cluster recreation (new sealed-secrets private key)

---

## 8. Component-Specific Details

### 8.1 Kafka (KRaft Mode)

**Why KRaft (not Zookeeper)?**
- Embedded consensus, no external quorum needed
- Simplified operations, fewer moving parts
- Confluent recommended for modern deployments

**Configuration:**
```yaml
metadata:
  name: kafka
  namespace: confluent
spec:
  replicas: 1  # local overlay; prod would be 3
  image:
    application: confluentinc/cp-server:8.2.2
  listeners:
    internal:
      enabled: true
      tls:
        secretRef: kafka-tls-secret
      authentication:
        type: plain  # mTLS + SASL/PLAIN
    external:
      enabled: true
      tls:
        secretRef: kafka-tls-secret
      authentication:
        type: plain
  configOverrides:
    server:
      - default.replication.factor=1
      - min.insync.replicas=1
      - log.retention.hours=168
      - auto.create.topics.enable=true
  dataVolumeClaimSpec:
    storageClassName: crc-csi-hostpath-provisioner
```

### 8.2 Connect (JDBC → SQL Server + Kerberos)

**Configuration:**
```yaml
metadata:
  name: connect
  namespace: confluent
spec:
  replicas: 1
  image:
    application: confluentinc/cp-server-connect:8.2.2
  configOverrides:
    connect:
      - value.converter=io.confluent.connect.avro.AvroConverter
      - key.converter=org.apache.kafka.connect.storage.StringConverter
  dependencies:
    kafka:
      bootstrapEndpoint: kafka:9092
  podTemplate:
    spec:
      containers:
      - name: connect
        volumeMounts:
        - name: keytab
          mountPath: /etc/krb5.keytab
          subPath: krb5.keytab
      volumes:
      - name: keytab
        secret:
          secretName: connect-keytab-secret  # Sealed in repo
```

**Connect Kerberos patch adds:**
- Keytab volume mount → `/etc/krb5.keytab`
- krb5.conf ConfigMap mount → `/etc/krb5.conf`
- JAAS config ConfigMap mount → `/opt/confluent/connect/etc/jaas.conf`
- JVM options: `-Dcom.sun.jndi.ldap.connect.pool=true -Dcom.sun.jndi.ldap.connect.pool.timeout=300`

**SQL Server connection flow:**
```
Connect pod → SSH reverse tunnel → Docker Desktop (Mac) 
           ↓
         [1433 port]
           ↓
      SQL Server (domain-joined to Samba AD DC)
           ↓
    SQL login: PSYNCOPATE\connect-svc (Kerberos SPNEGO)
```

### 8.3 Schema Registry

**Dependencies:** Kafka (for backend)

**Features:**
- Stores Avro/Protobuf/JSON schema definitions
- Connected to Kafka for durability
- Multi-tenant namespace support
- Metrics exported (Prometheus sidecar in future versions)

### 8.4 Control Center (ControlCenter Next Gen 2.5.0)

**Components:**
- Main UI pod (ControlCenter Next Gen)
- Prometheus sidecar (metrics scraping)
- Alertmanager sidecar (alert routing)

**Port Map:**
- 9021: Main UI/API
- 9090: Prometheus scrape endpoint (in-cluster only, NetworkPolicy restricts)
- 9093: Alertmanager (in-cluster only)

**Access:**
- OpenShift Route → `controlcenter.apps-crc.testing`
- Basic auth via C3 credentials (SealedSecret)

### 8.5 Flink (Apache Flink + CMF Operator)

**Architecture:**
1. **CMF Operator** (wave 0) - Installed via Helm, watches FlinkEnvironment/FlinkApplication CRDs
2. **FlinkEnvironment** (wave 1) - Cluster config, JobManager/TaskManager replicas, resources
3. **FlinkApplication** (wave 2) - Actual job: claims-processor

**claims-processor job:**
- **Source:** Kafka topic `sqlserver-claims-topic` (populated by Connect JDBC source)
- **Processing:** Stream claims through Flink stateful processing
- **Sink:** Output topic (TBD in repo)
- **Image:** `ghcr.io/mkurre/flink-jobs/claims-processor:sha-<GIT_SHA>` (auto-updated by ci-flink-build.yaml)

**Deployment Flow:**
```
Code change in flink-jobs/claims-processor/src/
  ↓
Push to main
  ↓
ci-flink-build.yaml
  - Maven build
  - Docker build/push
  - Update base/flink-jobs/flink-application.yaml with new image tag
  - Auto-commit to main
  ↓
ArgoCD detects manifest change
  ↓
flink-jobs Application syncs
  ↓
CMF operator provisions FlinkApplication
  ↓
Flink pods (JobManager + TaskManager) spin up with new image
  ↓
Job submissions from old pods drain, new pods scale up (rolling restart)
```

---

## 9. Network & Security Model

### 9.1 Network Policies

**Default-deny ingress pattern:**
- All pods in platform namespaces have default-deny NetworkPolicy
- Explicit allow rules for required traffic (pod-to-pod, external ingress)

**Policy Examples:**
- Allow Kafka → Kafka replication on 9092/9093/9094
- Allow SchemaRegistry → Kafka on 9092
- Allow Connect → Kafka/SR on 9092/8081
- Allow Argo CD → API server (Kubernetes API)
- Allow external clients → exposed Routes only

### 9.2 Pod Security Context (OpenShift SCCs)

**No custom SecurityContextConstraints:**
- Every CFK CR omits `runAsUser`/`fsGroup` in podTemplate
- OpenShift's built-in `restricted-v2` SCC auto-assigns UIDs from namespace range at admission
- Result: No privilege escalation, UID isolation per namespace

### 9.3 RBAC & Service Accounts

**Key RBAC bindings:**
- ArgoCD ServiceAccount: Cluster admin (reconciles all namespaces)
- CFK Operator ServiceAccount: Can create/manage Kafka CRs
- Confluent Platform ServiceAccounts: Limited to confluent namespace
- Flink ServiceAccounts: Limited to flink-jobs namespace

---

## 10. Operational Runbooks

### 10.1 Common Operations

| Operation | Procedure |
|-----------|-----------|
| **Deploy new Flink job** | Add source under flink-jobs/, push to main, ci-flink-build.yaml builds/pushes image, auto-updates manifest, ArgoCD syncs |
| **Update Kafka config** | Edit configOverrides in kafka.yaml or overlay patch, merge to main, ArgoCD syncs, self-healing restarts pods as needed |
| **Rotate TLS certs** | Delete certificate Secret + annotate Certificate CR, cert-manager reissues, pods pick up new secret on restart |
| **Promote local → prod** | Update confluent-platform app's source.path to overlays/prod, re-seal secrets against prod cluster, merge to main |
| **Reseal secrets** | After cluster recreation: fetch new sealed-secrets public cert, run seal-secrets.sh, commit regenerated manifests |
| **Approve CFK upgrade** | Check for pending InstallPlan, review changes, patch to approved=true |

### 10.2 Troubleshooting Entry Points

| Symptom | Cause | Fix |
|---------|-------|-----|
| Pod stuck `CreateContainerConfigError` | SCC violation (UID/GID) | Verify CFK CR doesn't hardcode runAsUser; let restricted-v2 assign |
| PVC stuck `Pending` | StorageClass typo | Confirm `storageClass.name: crc-csi-hostpath-provisioner` |
| Application `OutOfSync` forever | Kustomize builds differently than deployed | Check ignoreDifferences patterns in Application spec |
| Secret not decrypting | Sealed-secrets key changed (cluster recreated) | Re-seal all secrets with new key, commit regenerated manifests |
| Kafka broker not starting | Missing SASL credential | Verify kafka-sasl-sealed.yaml decrypted in-cluster |

---

## 11. Dependency Graph & Startup Order

```
cert-manager (bootstrap)
  ├─ Issues: kafka-tls, sr-tls, connect-tls, c3-tls, argocd-tls

sealed-secrets (bootstrap)
  ├─ Decrypts: kafka-sasl, c3-credentials, connect-keytab, flink-kafka-sasl

ArgoCD (bootstrap)
  └─ Manages all apps below

Wave 0:
  ├─ confluent-operator (CFK operator installation via OLM)
  └─ cmf-operator (Flink operator via Helm)

Wave 1:
  ├─ KRaftController (3 replicas, Raft consensus)
  ├─ FlinkEnvironment (Flink cluster bootstrap)
  └─ confluent-platform base components begin initialization

Wave 2:
  ├─ Kafka brokers (depend on KRaftController quorum established)
  ├─ FlinkApplication (depends on FlinkEnvironment)
  └─ flink-jobs app

Wave 3:
  └─ SchemaRegistry (depends on Kafka ready)

Wave 4:
  ├─ Connect (depends on Kafka + SchemaRegistry)
  └─ KafkaRestProxy (depends on Kafka)

Wave 5:
  └─ ControlCenter (depends on all above + Prometheus + Alertmanager sidecars)
```

---

## 12. Advanced Topics

### 12.1 Kerberos Integration (Optional, Documented)

This repo includes full Kerberos support for SQL Server integration:

- **Samba AD DC** (external, runs on Docker Desktop Mac)
  - Real Active Directory domain controller
  - Provides CLDAP, DNS, SMB, LDAP, Kerberos KDC services
  - Reverse-SSH-tunneled into cluster (port 88 for KDC, 1433 for SQL Server)

- **Connect JDBC Source Connector**
  - Keytab-based Kerberos authentication
  - SQL Server login: `PSYNCOPATE\connect-svc` (domain principal)
  - krb5.conf + JAAS config via ConfigMaps
  - Volumes: keytab, krb5.conf, jaas.conf, truststore

- **Setup Script:** `scripts/kerberos/setup-kerberos.sh`
  - Provisions Samba AD DC on Docker Desktop
  - Creates Connect service account + keytab
  - Domain joins SQL Server
  - Generates Kubernetes manifests for krb5.conf, JAAS, truststore

### 12.2 Image Management

**Image Tags and Updates:**

- **Overlay images:** `overlays/local/kustomization.yaml` sets version tags
- **Flink images:** Auto-updated by `ci-flink-build.yaml` workflow
- **Upstream image updates:** Edit overlay, commit, ArgoCD re-syncs

**Multi-environment image strategy:**
```yaml
# overlays/local/kustomization.yaml
images:
  - name: confluentinc/cp-server
    newTag: "8.2.2"  # Local dev version

# overlays/prod/kustomization.yaml
images:
  - name: confluentinc/cp-server
    newTag: "8.2.2"  # Could be pinned to different version
```

### 12.3 Ignoring Reconciliation Differences

ArgoCD's `ignoreDifferences` field prevents false `OutOfSync` states caused by controller-injected fields:

```yaml
spec:
  ignoreDifferences:
    - group: platform.confluent.io
      kind: Kafka
      jsonPointers:
        - /status  # Ignore controller-populated status fields
    - group: platform.confluent.io
      kind: SchemaRegistry
      jsonPointers:
        - /status
```

---

## 13. File Reference Guide

### 13.1 Critical Files by Function

| File | Purpose | Edited By |
|------|---------|-----------|
| `apps/app-of-apps.yaml` | ArgoCD root app, points to all child apps | DevOps eng |
| `base/confluent-platform/kafka.yaml` | Kafka broker spec, auth, listeners, replicas | Platform eng |
| `overlays/local/kustomization.yaml` | Local dev config (1 broker, 1 replica) | Developers |
| `overlays/prod/kustomization.yaml` | Prod config (3 brokers, min.insync=2) | DevOps eng (with approval) |
| `.github/workflows/ci-validate.yaml` | Pre-merge validation (lint, build, scan) | DevOps eng |
| `.github/workflows/ci-flink-build.yaml` | Flink Maven → Docker → image tag update | Flink developers |
| `.github/workflows/cd-argocd-sync.yaml` | Post-merge deployment via ArgoCD | Auto-triggered |
| `scripts/bootstrap.sh` | One-time cluster initialization | DevOps eng (one-time) |
| `scripts/seal-secrets.sh` | Encrypt plaintext Secrets → SealedSecrets | DevOps eng (cluster lifecycle) |
| `base/confluent-platform/secrets/*.yaml` | Sealed Kafka/C3/Connect credentials | Generated by seal-secrets.sh |
| `docs/architecture.md` | This repo's design & sync-wave order | System documentation |
| `docs/runbook.md` | Operational procedures | System documentation |

### 13.2 Commonly Modified Files

**For Kafka configuration changes:**
```
base/confluent-platform/kafka.yaml
├─ spec.replicas
├─ spec.configOverrides.server
├─ spec.dataVolumeClaimSpec
└─ spec.tls.secretRef

overlays/local/kafka-patch.yaml (for local-specific overrides)
overlays/prod/kafka-patch.yaml (for prod-specific overrides)
```

**For Flink job updates:**
```
flink-jobs/claims-processor/src/main/java/...  (Code)
  ↓
flink-jobs/claims-processor/Dockerfile  (Image build)
  ↓
base/flink-jobs/flink-application.yaml  (Image ref, auto-updated by ci-flink-build.yaml)
  ↓
base/flink-jobs/kustomization.yaml  (Lists application manifests)
```

---

## 14. Key Configuration Patterns

### 14.1 Multi-Listener Kafka Pattern

```yaml
listeners:
  internal:
    enabled: true
    tls:
      secretRef: kafka-tls-secret
    authentication:
      type: plain
  external:
    enabled: true
    tls:
      secretRef: kafka-tls-secret
    authentication:
      type: plain
```

**Result:**
- Internal (pod-to-pod): `kafka:9092` (SASL/PLAIN over TLS)
- External (client): `kafka.apps-crc.testing:9094` (SASL/PLAIN over TLS + Route)

### 14.2 SealedSecret Decryption Flow

```yaml
# base/confluent-platform/secrets/kafka-sasl-sealed.yaml
apiVersion: bitnami.com/v1alpha1
kind: SealedSecret
metadata:
  name: kafka-sasl-secret
spec:
  encryptedData:
    username: AgBvs+3...  # Encrypted with cluster's public key
    password: AgBk8xZ...
  template:
    type: Opaque
```

**At sync time:**
1. ArgoCD applies SealedSecret to cluster
2. sealed-secrets controller decrypts with private key → generates `Secret: kafka-sasl-secret`
3. Kafka pod mounts Secret as volume
4. Kafka process reads credentials from file

### 14.3 Kustomize Patch Pattern (Kerberos Example)

**Original (base):**
```yaml
# base/confluent-platform/connect.yaml
apiVersion: platform.confluent.io/v1beta1
kind: Connect
metadata:
  name: connect
spec:
  replicas: 1
  ...
```

**Patch (applied by base/confluent-platform/kustomization.yaml):**
```yaml
# base/confluent-platform/connect-kerberos-patch.yaml
- op: add
  path: /spec/podTemplate/spec/containers/0/volumeMounts
  value:
    - name: keytab
      mountPath: /etc/krb5.keytab
      subPath: krb5.keytab
    - name: krb5-conf
      mountPath: /etc/krb5.conf
      subPath: krb5.conf
- op: add
  path: /spec/podTemplate/spec/volumes
  value:
    - name: keytab
      secret:
        secretName: connect-keytab-secret
    - name: krb5-conf
      configMap:
        name: connect-krb5-configmap
```

**Result:** Connect pod gains keytab + krb5.conf without editing connect.yaml

---

## 15. Data Flow Example: Kafka → Connect → SQL Server

```
Cluster 1: OpenShift (this repo)
├─ Kafka broker (9092 SASL, 9094 external)
├─ Connect pod
│  └─ JDBC Source Connector (polls SQL Server via Kerberos)
│
├─ SchemaRegistry (schemas stored here)
└─ Flink pod
   └─ claims-processor job (consumes from kafka:9092)

Cluster 2: Docker Desktop (external)
├─ Samba AD DC (LDAP, Kerberos KDC, DNS)
│  └─ domain: PSYNCOPATE.COM
└─ SQL Server
   └─ Database: claims-db (source of truth)

Flow:
1. SQL Server client (Connect) requests TGT from Samba AD DC KDC
   └─ SSH reverse tunnel carries port 88 (KDC) from mac to cluster
2. Connect logs in with SPNEGO embedded in TDS (SQL protocol)
   └─ SQL Server validates Kerberos ticket from AD DC
3. Connect fetches rows from SQL Server
4. Rows → Avro → Kafka topic `sqlserver-claims-topic`
5. Flink job consumes from topic
6. Flink processes claims (stateful aggregation, windowing, etc.)
7. Output → sink topic (or external system)
```

---

## 16. Environment Differences: Local vs. Prod

| Aspect | Local (CRC) | Production |
|--------|------------|----------|
| **KRaft replicas** | 1 | 3 |
| **Kafka brokers** | 1 | 3 |
| **default.replication.factor** | 1 | 3 |
| **min.insync.replicas** | 1 | 2 |
| **auto.create.topics** | true | false |
| **CPU/Memory requests** | Minimal (dev) | Conservative (high availability) |
| **SchemaRegistry replicas** | 1 | 3 |
| **Connect replicas** | 1 | 3 |
| **ControlCenter replicas** | 1 | 3 |
| **Flink JobManager replicas** | 1 | 3 |
| **Flink TaskManager replicas** | 1 | Multiple |
| **TLS cert SAN** | *.apps-crc.testing | Production domain |
| **Sealed-secrets key** | Local cluster key | Prod cluster key |
| **Repository branch** | main (promoted from feature branches) | main (read-only via prod Application) |

---

## 17. Deployment Validation Checklist

### Pre-Deployment (ci-validate.yaml)
- [ ] YAML lint passes (200 char line limit)
- [ ] All kustomizations build successfully
- [ ] No plaintext `kind: Secret` objects in repo
- [ ] trufflehog detects no leaked credentials
- [ ] All Application CRs have syncPolicy/repoURL/destination

### Post-Bootstrap (operators installed)
- [ ] `oc get csv -n confluent-operator` shows CFK operator running
- [ ] `oc get csv -n cmf-operator` shows CMF operator running (if Flink enabled)
- [ ] `oc get nodes` shows all nodes ready
- [ ] `oc get storageclass` includes `crc-csi-hostpath-provisioner`

### Post-ArgoCD Sync (platform running)
- [ ] `oc get kafka kafka -n confluent` shows all replicas ready
- [ ] `oc get pod -n confluent | grep kafka` shows all brokers running
- [ ] Kafka logs show "Leader elected" messages
- [ ] SchemaRegistry logs show successful Kafka connection
- [ ] Connect logs show no errors
- [ ] ControlCenter accessible via Route, all components visible
- [ ] Flink JobManager pod running, REST API accessible
- [ ] Test topic creation: `oc exec kafka-0 -c kafka -n confluent -- kafka-topics.sh --list --bootstrap-server kafka:9092 --command-config /opt/confluentinc/etc/kafka/client.properties`

---

## 18. CI/CD Summary Matrix

| Workflow | Trigger | Environment | Runs | Status | Output |
|----------|---------|-------------|------|--------|--------|
| ci-validate.yaml | PR, push any branch | GitHub Cloud | lint, kustomize-build, secret-scan, argocd-app-validate | Pass/Fail blocks merge | Green checkmark or required fixes |
| ci-flink-build.yaml | push main, flink-jobs/** change | GitHub Cloud | Maven build, Docker build/push, image tag auto-commit | Pass/Fail async | Image pushed to ghcr.io, manifest updated |
| cd-argocd-sync.yaml | push main or workflow_dispatch | Self-hosted (Mac) | ArgoCD sync wave-by-wave | Pass/Fail | Pods reconciled to target state |

---

## Conclusion

This repository represents a **production-grade GitOps implementation** with:

✅ **Automated validation** (ci-validate.yaml) preventing invalid configs from reaching main
✅ **Container image pipeline** (ci-flink-build.yaml) automating Flink job deployments
✅ **Declarative infrastructure** using Kustomize for multi-environment support
✅ **Secrets encryption** via SealedSecrets with per-cluster keys
✅ **High availability** potential (prod overlays support 3-node Kafka)
✅ **TLS everywhere** with auto-renewing certificates
✅ **Enterprise features** (ControlCenter, SchemaRegistry, Connect) for full Kafka ecosystem
✅ **Stream processing** via Flink + CMF operator integration
✅ **Operational runbooks** documenting common tasks and troubleshooting

The sync-wave design ensures correct boot order, and ArgoCD's self-healing keeps the cluster converged to Git state at all times.
