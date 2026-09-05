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

Boot-managed libraries (Spring Kafka, Mongo, Lettuce, Micrometer, OTel) take their versions from the 4.1.1 BOM. Confluent and Avro are pinned explicitly because they are not in that BOM.

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

All endpoints and secrets are `${ENV:default}` placeholders. Copy `.env.example` and export the variables (or inject them in k8s). Never commit real Confluent / Mongo / Redis credentials.

Important knobs:

- `KAFKA_BOOTSTRAP_SERVERS`, `SCHEMA_REGISTRY_URL`, `SCHEMA_REGISTRY_USER_INFO`
- `KAFKA_INBOUND_TOPIC`, `KAFKA_OUTBOUND_TOPIC`, `KAFKA_CONSUMER_GROUP`
- `KAFKA_LISTENER_CONCURRENCY` (default `2`), `KAFKA_MAX_POLL_RECORDS` (default `25`)
- `MONGODB_URI`, `REDIS_HOST`, `REDIS_PORT`, `REDIS_PASSWORD`
- `REDIS_LOCK_TTL`, `REDIS_DEDUP_TTL`
- `OTEL_EXPORTER_OTLP_TRACES_ENDPOINT` (optional collector)

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
