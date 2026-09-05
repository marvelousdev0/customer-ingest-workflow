package com.acme.customeringest.redis;

import com.acme.customeringest.common.AppConstants;
import com.acme.customeringest.config.AppProperties;
import java.time.Duration;
import java.util.Optional;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

/**
 * Per-customer mutex: {@code lock:customer:{customerId}} via SET NX + TTL. Only serializes work for
 * the same customerId; other customers proceed immediately. Always release in {@code finally} via
 * {@link LockLease#close()}.
 */
@Component
public class CustomerIdLock {

  private static final Logger LOG = LoggerFactory.getLogger(CustomerIdLock.class);

  private final StringRedisTemplate redis;
  private final Duration lockTtl;
  private final int maxAttempts;
  private final Duration backoff;

  public CustomerIdLock(StringRedisTemplate redis, AppProperties properties) {
    this.redis = redis;
    this.lockTtl = properties.redis().lockTtl();
    this.maxAttempts = properties.redis().lockMaxAttempts();
    this.backoff = properties.redis().lockBackoff();
  }

  public Optional<LockLease> acquire(String customerId) {
    String key = lockKey(customerId);
    String token = UUID.randomUUID().toString();
    for (int attempt = 1; attempt <= maxAttempts; attempt++) {
      Boolean acquired = redis.opsForValue().setIfAbsent(key, token, lockTtl);
      if (Boolean.TRUE.equals(acquired)) {
        LOG.debug("Acquired customerId lock key={} attempt={}", key, attempt);
        return Optional.of(new LockLease(redis, key, token));
      }
      LOG.debug("CustomerId lock contended key={} attempt={}/{}", key, attempt, maxAttempts);
      if (attempt < maxAttempts) {
        RetryBackoff.park(backoff);
      }
    }
    LOG.warn("Could not acquire customerId lock key={} after {} attempts", key, maxAttempts);
    return Optional.empty();
  }

  public static String lockKey(String customerId) {
    return AppConstants.Redis.CUSTOMER_LOCK_KEY_PREFIX + customerId;
  }

  public static final class LockLease implements AutoCloseable {

    private final StringRedisTemplate redis;
    private final String key;
    private final String token;
    private boolean closed;

    LockLease(StringRedisTemplate redis, String key, String token) {
      this.redis = redis;
      this.key = key;
      this.token = token;
    }

    @Override
    public void close() {
      if (closed) {
        return;
      }
      closed = true;
      String current = redis.opsForValue().get(key);
      if (token.equals(current)) {
        redis.delete(key);
      }
    }
  }
}
