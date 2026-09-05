package com.acme.customeringest.redis;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.acme.customeringest.common.AppConstants;
import com.acme.customeringest.config.AppProperties;
import java.time.Duration;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ValueOperations;

@ExtendWith(MockitoExtension.class)
class DuplicateMessageGuardTest {

  private static final String TOPIC = "in";

  @Mock private StringRedisTemplate redis;

  @Mock private ValueOperations<String, String> valueOps;

  private DuplicateMessageGuard guard;

  @BeforeEach
  void setUp() {
    guard = new DuplicateMessageGuard(redis, properties());
  }

  @Test
  void offsetKeyMarksAndDetectsDuplicate() {
    String offsetKey = DuplicateMessageGuard.offsetKey(TOPIC, 0, 7);
    when(redis.opsForValue()).thenReturn(valueOps);
    when(redis.hasKey(offsetKey)).thenReturn(false, true);

    assertThat(guard.alreadyProcessed(TOPIC, 0, 7, null)).isFalse();
    guard.markProcessed(TOPIC, 0, 7, null);
    assertThat(guard.alreadyProcessed(TOPIC, 0, 7, null)).isTrue();

    verify(valueOps)
        .set(
            eq(offsetKey),
            eq(AppConstants.Redis.DEDUP_MARKER),
            eq(AppConstants.Redis.DEFAULT_DEDUP_TTL));
  }

  @Test
  void idempotencyHeaderIsHonoredWithoutMongoSideEffectsHere() {
    when(redis.hasKey(DuplicateMessageGuard.offsetKey(TOPIC, 1, 2))).thenReturn(false);
    when(redis.hasKey(DuplicateMessageGuard.idempotencyKey("biz-1"))).thenReturn(true);

    assertThat(guard.alreadyProcessed(TOPIC, 1, 2, "biz-1")).isTrue();
    verify(valueOps, never()).set(any(), any(), any(Duration.class));
  }

  private static AppProperties properties() {
    return new AppProperties(
        new AppProperties.Kafka("in", "out"),
        new AppProperties.Redis(
            AppConstants.Redis.DEFAULT_LOCK_TTL,
            3,
            Duration.ofMillis(1),
            AppConstants.Redis.DEFAULT_DEDUP_TTL),
        new AppProperties.Mongo(1, 2));
  }
}
