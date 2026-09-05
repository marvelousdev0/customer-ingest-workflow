package com.acme.customeringest.common;

import java.time.Duration;

/**
 * Single home for application-wide literals so Kafka, Redis, Mongo, and tracing code do not scatter
 * magic strings or numbers.
 */
public final class AppConstants {

  private AppConstants() {
    throw new AssertionError("Utility class");
  }

  public static final class Kafka {

    public static final String DEFAULT_INBOUND_TOPIC = "customer.ingest.in";
    public static final String DEFAULT_OUTBOUND_TOPIC = "customer.ingest.out";
    public static final String LISTENER_CONTAINER_FACTORY = "kafkaListenerContainerFactory";
    public static final int DEFAULT_MAX_POLL_RECORDS = 25;
    public static final int DEFAULT_LISTENER_CONCURRENCY = 2;
    public static final boolean ENABLE_AUTO_COMMIT = false;
    public static final Duration NACK_SLEEP = Duration.ZERO;

    private Kafka() {
      throw new AssertionError("Utility class");
    }
  }

  public static final class Redis {

    public static final String CUSTOMER_LOCK_KEY_PREFIX = "lock:customer:";
    public static final String OFFSET_DEDUP_KEY_PREFIX = "dedup:offset:";
    public static final String IDEMPOTENCY_DEDUP_KEY_PREFIX = "dedup:idemp:";
    public static final String IDEMPOTENCY_HEADER = "idempotency-key";
    public static final String DEDUP_MARKER = "1";
    public static final Duration DEFAULT_LOCK_TTL = Duration.ofSeconds(10);
    public static final int DEFAULT_LOCK_MAX_ATTEMPTS = 5;
    public static final Duration DEFAULT_LOCK_BACKOFF = Duration.ofMillis(25);
    public static final Duration DEFAULT_DEDUP_TTL = Duration.ofDays(7);

    private Redis() {
      throw new AssertionError("Utility class");
    }
  }

  public static final class Mongo {

    public static final String CUSTOMERS_COLLECTION = "customers";
    public static final int DEFAULT_POOL_MIN = 10;
    public static final int DEFAULT_POOL_MAX = 50;
    public static final int MAX_WAIT_TIME_SECONDS = 5;
    public static final int MAX_IDLE_TIME_SECONDS = 60;

    private Mongo() {
      throw new AssertionError("Utility class");
    }
  }

  public static final class Tracing {

    public static final String SPAN_NAME = "customer.ingest.process";
    public static final String TAG_MESSAGING_SYSTEM = "messaging.system";
    public static final String TAG_MESSAGING_DESTINATION = "messaging.destination";
    public static final String TAG_MESSAGING_PARTITION = "messaging.kafka.partition";
    public static final String TAG_MESSAGING_OFFSET = "messaging.kafka.offset";
    public static final String MESSAGING_SYSTEM_KAFKA = "kafka";

    private Tracing() {
      throw new AssertionError("Utility class");
    }
  }
}
