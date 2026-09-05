# Customer Ingest Workflow

High-throughput Spring Boot Kafka ingestion service: Confluent Avro in, Redis idempotency + per-`customerId` lock, MongoDB upsert, Avro response out. Trace IDs stay on the record from consume through produce.

## Versions (latest stable GA, Sep 2026)

| Component | Version |
| --- | --- |
| Spring Boot | **4.1.1** |
| Java toolchain | **26** |
| Gradle wrapper | **9.7.1** |
| Apache Kafka clients (Boot BOM) | **4.2.1** |
| Spring Kafka (Boot BOM) | **4.1.1** |
| Confluent `kafka-avro-serializer` | **8.3.0** |
| Apache Avro + `avro-tools` codegen | **1.12.2** |
| MongoDB Java driver (Boot BOM) | **5.8.1** |
| Lettuce (Boot BOM) | **7.5.2.RELEASE** |
| Micrometer Tracing (Boot BOM) | **1.7.1** |
| OpenTelemetry (Boot BOM) | **1.62.0** |
| Testcontainers (optional) | **2.0.5** |
| Spring Cloud (Oakwood) | **2025.1.3** |
| HashiCorp Vault (Spring Cloud Vault) | **5.0.2** (via Cloud BOM) |
| LaunchDarkly Java Server SDK | **7.16.0** |

Boot-managed libraries (Spring Kafka, Mongo, Lettuce, Micrometer, OTel) take their versions from the 4.1.1 BOM. Confluent, Avro, and LaunchDarkly are pinned explicitly because they are not in that BOM. Spring Cloud Vault is managed by the Spring Cloud BOM.

## How it works

```
inbound Avro topic  →  batch listener (25 records / thread)
        → parse + validate
        → Redis dedup (skip if seen)
        → Redis lock on customerId
        → MongoDB upsert
        → outbound Avro topic (PROCESSED only)
```

### 2 pods / 4 partitions / 25 per thread

- Deployment replicas: **2** (`k8s/deployment.yaml`).
- Shared `group.id` (`KAFKA_CONSUMER_GROUP`).
- `spring.kafka.listener.concurrency=2` per pod → 4 threads for 4 partitions.
- `max.poll.records=25` and a **batch** listener (`List<ConsumerRecord>`).
- Manual ack (`enable-auto-commit=false`, `ack-mode=manual`) after the batch succeeds.

If one `customerId` cannot get a lock, that record is **not** acked (batch `nack` from the first retryable index). Other messages in the same batch are still processed. Redis dedup makes redelivery of already-written records a no-op.

### Redis keys

| Purpose | Key | Notes |
| --- | --- | --- |
| Dedup (Kafka identity) | `dedup:offset:{topic}:{partition}:{offset}` | Safest for the same record being redelivered |
| Dedup (business) | `dedup:idemp:{value}` | Optional inbound header `idempotency-key` |
| Lock | `lock:customer:{customerId}` | `SET NX` + TTL; retry/backoff; release in `finally` |

**Duplicate policy:** skip Mongo write and **do not** republish to the outbound topic. Log the skip with `traceId`.

### Tracing

Micrometer Tracing + OpenTelemetry (`spring-boot-starter-opentelemetry`). A per-record interceptor:

1. Extracts W3C `traceparent` / `tracestate` and B3 headers from the inbound Kafka record.
2. Opens a child span so SLF4J MDC `traceId` and `spanId` stay set through dedup → lock → Mongo → produce.
3. Injects the same headers onto the outbound Kafka record.

Log pattern (see `logback-spring.xml`): `[%X{traceId:-},%X{spanId:-}]`.

PII: `customerId` is logged at INFO. Phone/address only at DEBUG.

## Configure

### HashiCorp Vault

Deployed environments load Kafka / Schema Registry / Mongo / Redis credentials from **HashiCorp Vault** KV via Spring Cloud Vault (`spring.config.import=vault://`).

| Setting | Placeholder default | Notes |
| --- | --- | --- |
| `VAULT_URI` | `http://127.0.0.1:8200` | Vault HTTP API address |
| `VAULT_AUTHENTICATION` | `TOKEN` | Switch later to `KUBERNETES` / `APPROLE` as needed |
| `VAULT_TOKEN` | `00000000-0000-0000-0000-000000000000` | Dev/placeholder token only |
| `VAULT_KV_BACKEND` | `secret` | KV mount path |
| `VAULT_KV_DEFAULT_CONTEXT` | `customer-ingest-workflow` | Path under the mount |
| `VAULT_CONFIG_IMPORT` | `optional:vault://` | Use `vault://` + `VAULT_FAIL_FAST=true` in prod |

Store flat Spring property keys at `secret/data/customer-ingest-workflow` (KV v2). See `vault/secrets.example.json`. Vault properties override `application.yml` when present.

The `local` profile sets `spring.cloud.vault.enabled=false` so `bootRun` works without a Vault server; use `.env` fallbacks instead.

### LaunchDarkly feature flags

Feature flags are evaluated via the LaunchDarkly Java server SDK. The SDK key is a secret (`app.launchdarkly.sdk-key` in Vault, or `LAUNCHDARKLY_SDK_KEY`). Placeholder / local runs stay **offline** and return code defaults.

| Flag key | Default | Behavior |
| --- | --- | --- |
| `customer-ingest.outbound-publish` | `true` | When false, Mongo write + dedup still happen; Kafka outbound publish is skipped |

Use `FeatureFlags` in code:

```java
if (featureFlags.isOutboundPublishEnabled(customerId)) { ... }
if (featureFlags.isEnabledForCustomer("my-flag", customerId, false)) { ... }
```

Create the flag in the LaunchDarkly dashboard with context kind `customer` (attribute `customerId`) for targeting.

### Local / non-secret knobs

Copy `.env.example` and export the variables. Never commit real Confluent / Mongo / Redis / Vault credentials.

- `KAFKA_INBOUND_TOPIC`, `KAFKA_OUTBOUND_TOPIC`, `KAFKA_CONSUMER_GROUP`
- `KAFKA_LISTENER_CONCURRENCY` (default `2`), `KAFKA_MAX_POLL_RECORDS` (default `25`)
- `REDIS_LOCK_TTL`, `REDIS_DEDUP_TTL`
- Local-only fallbacks: `KAFKA_BOOTSTRAP_SERVERS`, `SCHEMA_REGISTRY_URL`, `MONGODB_URI`, `REDIS_*`

## Run

```bash
# optional: export $(grep -v '^#' .env | xargs)
./gradlew bootRun
# or
./gradlew bootRun --args='--spring.profiles.active=local'
```

### Avro → Java POJOs (compile-time only)

`.avsc` files under `src/main/avro/` are the schema source of truth. They are **not** loaded at runtime. `./gradlew generateAvroJava` (official avro-tools 1.12.2) writes SpecificRecord classes to `build/generated-main-avro-java/`:

- `CustomerIngestEvent` + nested `Address` — inbound listener value type
- `CustomerProcessedEvent` — outbound publisher value type

The batch listener takes `List<ConsumerRecord<String, CustomerIngestEvent>>`. Confluent `KafkaAvroDeserializer` runs with `specific.avro.reader=true` (wrapped in `ErrorHandlingDeserializer`). The producer sends `CustomerProcessedEvent` via `KafkaAvroSerializer`. Mongo uses a separate `@Document` mapped from the ingest POJO.

```bash
./gradlew classes
./gradlew test
```

Unit tests (Mockito) cover duplicate skip, customerId lock serialization, and Mongo + outbound publish on success. Testcontainers modules are on the classpath but **not executed here** — Docker was not available when this repo was scaffolded.

Health: `GET /actuator/health` (liveness/readiness enabled for k8s).
