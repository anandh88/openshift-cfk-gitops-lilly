# Architecture

## Stack overview

```
                              ┌─────────────────────────────┐
                              │        Argo CD (argocd)      │
                              │   platform-root (app-of-apps)│
                              └───────────────┬──────────────┘
                                               │ manages
              ┌────────────────────────────────┼────────────────────────────────┐
              │                                │                                │
              ▼                                ▼                                ▼
  ┌───────────────────────┐      ┌──────────────────────────┐      ┌──────────────────────┐
  │ confluent-operator app │      │  confluent-platform app   │      │   flink-jobs app       │
  │ (sync-wave 0)           │      │  (sync-wave 1)             │      │   (sync-wave 2)        │
  │ ns: confluent-operator  │      │  ns: confluent             │      │   ns: flink-jobs        │
  └───────────┬────────────┘      └──────────────┬────────────┘      └───────────┬───────────┘
              │                                  │                               │
              ▼                                  ▼                               ▼
  ┌───────────────────────┐   ┌────────────────────────────────────┐  ┌───────────────────────┐
  │ CFK Operator (OLM)     │   │ KRaftController (3)                 │  │ FlinkEnvironment        │
  │ confluent-for-         │   │ Kafka (3)                            │  │ FlinkApplication        │
  │ kubernetes             │   │ SchemaRegistry / Connect             │  │ (claims-processor)      │
  └────────────────────────┘   │ KafkaRestProxy / ControlCenter       │  └───────────┬───────────┘
                                └───────────────┬──────────────────────┘              │
                                                │ Kafka bootstrap (mTLS + SASL/PLAIN)  │
                                                └───────────────────────────────────────┘

  cmf-operator app (sync-wave 1, ns: cmf-operator) → CMF operator (Flux HelmRelease)
  manages FlinkEnvironment/FlinkApplication CRDs consumed by flink-jobs.
```

## Namespace design

| Namespace           | Purpose                                              |
|---------------------|-------------------------------------------------------|
| `argocd`             | Argo CD control plane, app-of-apps root               |
| `cert-manager`       | Cluster CA + all leaf certificate issuance             |
| `confluent-operator` | CFK operator (OLM Subscription + OperatorGroup)        |
| `confluent`          | Kafka, KRaft, SchemaRegistry, Connect, C3, RestProxy   |
| `cmf-operator`       | Confluent Manager for Apache Flink operator            |
| `flink-jobs`         | FlinkEnvironment + FlinkApplication workloads          |

## Component communication map

| From              | To                | Port(s)        | Protocol         |
|-------------------|--------------------|-----------------|------------------|
| KRaftController    | Kafka              | 9092, 9093       | TLS + SASL/PLAIN |
| Kafka              | Kafka (replication)| 9092–9094        | TLS + SASL/PLAIN |
| SchemaRegistry     | Kafka               | 9092             | TLS + SASL/PLAIN |
| Connect            | Kafka, SchemaRegistry | 9092, 8081     | TLS + SASL/PLAIN |
| ControlCenter      | Kafka, SR, Connect  | 9092, 8081, 8083 | TLS + basic auth |
| FlinkApplication   | Kafka                | 9092           | TLS + SASL/PLAIN |
| Argo CD            | All namespaces (RBAC)| n/a            | Kubernetes API   |

## TLS flow

1. `selfsigned-bootstrap` ClusterIssuer mints the `platform-root-ca` CA certificate.
2. `platform-ca-issuer` ClusterIssuer signs off that root CA secret.
3. Every component certificate (`kafka-tls`, `schemaregistry-tls`, `connect-tls`,
   `controlcenter-tls`, `restproxy-tls`, `argocd-server-tls`) is issued by
   `platform-ca-issuer`, giving the whole platform one trust chain.
4. Internal (pod-to-pod) and external (Route) traffic both terminate on the
   same certificate per component — internal via the in-cluster DNS SAN,
   external via the `*.apps-crc.testing` wildcard SAN on `kafka-tls`, or the
   component-specific `apps-crc.testing` SAN on the others.

## GitOps workflow

1. Engineers open a PR against `main`; `ci-validate.yaml` lints YAML, builds
   every kustomization, scans for accidentally-committed plaintext secrets,
   and checks every Argo CD `Application` has a syncPolicy/repoURL/destination.
2. `ci-flink-build.yaml` builds and pushes the claims-processor image on
   changes under `flink-jobs/**`, then bumps `base/flink-jobs/flink-application.yaml`
   with the new image tag via an automated commit.
3. Merges to `main` are picked up by Argo CD's automated sync (`platform-root`
   app-of-apps), or explicitly triggered by `cd-argocd-sync.yaml`.
4. Argo CD reconciles each child Application in sync-wave order (see below).

## Sync wave order

| Wave | Application / Resource                          | Notes                              |
|------|--------------------------------------------------|--------------------------------------|
| 0    | confluent-operator app, cmf-operator HelmRelease  | Operators must exist before CRs      |
| 1    | confluent-platform app, cmf-operator app          | Kafka stack + CMF operator           |
| 1    | KRaftController, FlinkEnvironment                 | Quorum before brokers, env before job|
| 2    | Kafka, FlinkApplication, flink-jobs app           | Depends on KRaft quorum              |
| 3    | SchemaRegistry                                    | Depends on Kafka                     |
| 4    | Connect, KafkaRestProxy                           | Depend on Kafka + SchemaRegistry     |
| 5    | ControlCenter                                     | Depends on everything above          |

## Port reference

| Component        | Port  | Purpose                     |
|-------------------|-------|-------------------------------|
| Kafka              | 9092  | Internal listener (SASL/TLS)  |
| Kafka              | 9093  | KRaft controller listener      |
| Kafka              | 9094  | External listener (SASL/TLS)  |
| SchemaRegistry     | 8081  | REST API                       |
| Connect            | 8083  | REST API                       |
| KafkaRestProxy     | 8082  | REST API                       |
| ControlCenter      | 9021  | Web UI                         |
| Argo CD            | 443   | Server UI/API (https, route)   |
| Flink JobManager   | 8081  | Flink REST/UI                  |
