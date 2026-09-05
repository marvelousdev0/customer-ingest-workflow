package com.acme.customeringest.redis;

import com.acme.customeringest.common.AppConstants;
import com.acme.customeringest.config.AppProperties;
import java.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

/**
 * Idempotency keys (SET after successful processing, GET before work):
 *
 * <ul>
 *   <li>{@code dedup:offset:{topic}:{partition}:{offset}} — safest for at-least-once redelivery of
 *       the same Kafka record.
 *   <li>{@code dedup:idemp:{idempotency-key}} — optional business key from the {@code
 *       idempotency-key} Kafka header when producers supply one.
 * </ul>
 *
 * Duplicates are logged and skipped: no Mongo write and no outbound republish.
 */
@Component
public class DuplicateMessageGuard {

  private static final Logger LOG = LoggerFactory.getLogger(DuplicateMessageGuard.class);

  private final StringRedisTemplate redis;
  private final Duration dedupTtl;

  public DuplicateMessageGuard(StringRedisTemplate redis, AppProperties properties) {
    this.redis = redis;
    this.dedupTtl = properties.redis().dedupTtl();
  }

  public boolean alreadyProcessed(String topic, int partition, long offset, String idempotencyKey) {
    String offsetKey = offsetKey(topic, partition, offset);
    if (Boolean.TRUE.equals(redis.hasKey(offsetKey))) {
      LOG.info("Skipping duplicate Kafka record key={}", offsetKey);
      return true;
    }
    if (StringUtils.hasText(idempotencyKey)) {
      String idempKey = idempotencyKey(idempotencyKey);
      if (Boolean.TRUE.equals(redis.hasKey(idempKey))) {
        LOG.info("Skipping duplicate by idempotency-key header key={}", idempKey);
        return true;
      }
    }
    return false;
  }

  public void markProcessed(String topic, int partition, long offset, String idempotencyKey) {
    redis
        .opsForValue()
        .set(offsetKey(topic, partition, offset), AppConstants.Redis.DEDUP_MARKER, dedupTtl);
    if (StringUtils.hasText(idempotencyKey)) {
      redis
          .opsForValue()
          .set(idempotencyKey(idempotencyKey), AppConstants.Redis.DEDUP_MARKER, dedupTtl);
    }
  }

  public static String offsetKey(String topic, int partition, long offset) {
    return AppConstants.Redis.OFFSET_DEDUP_KEY_PREFIX + topic + ":" + partition + ":" + offset;
  }

  public static String idempotencyKey(String headerValue) {
    return AppConstants.Redis.IDEMPOTENCY_DEDUP_KEY_PREFIX + headerValue;
  }
}
