package com.acme.customeringest.redis;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.acme.customeringest.common.AppConstants;
import com.acme.customeringest.config.AppProperties;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ValueOperations;

@ExtendWith(MockitoExtension.class)
class CustomerIdLockTest {

  private static final String CUSTOMER_ID = "c1";
  private static final String SAME_CUSTOMER_ID = "same";
  private static final int LOCK_MAX_ATTEMPTS = 3;

  @Mock private StringRedisTemplate redis;

  @Mock private ValueOperations<String, String> valueOps;

  private CustomerIdLock lock;

  @BeforeEach
  void setUp() {
    when(redis.opsForValue()).thenReturn(valueOps);
    lock = new CustomerIdLock(redis, properties());
  }

  @Test
  void acquireSucceedsOnSetNx() {
    when(valueOps.setIfAbsent(
            eq(CustomerIdLock.lockKey(CUSTOMER_ID)),
            anyString(),
            eq(AppConstants.Redis.DEFAULT_LOCK_TTL)))
        .thenReturn(true);

    Optional<CustomerIdLock.LockLease> lease = lock.acquire(CUSTOMER_ID);

    assertThat(lease).isPresent();
    lease.orElseThrow().close();
  }

  @Test
  void acquireFailsAfterRetriesWhenContended() {
    when(valueOps.setIfAbsent(anyString(), anyString(), any(Duration.class))).thenReturn(false);

    assertThat(lock.acquire(CUSTOMER_ID)).isEmpty();
    verify(valueOps, times(LOCK_MAX_ATTEMPTS))
        .setIfAbsent(eq(CustomerIdLock.lockKey(CUSTOMER_ID)), anyString(), any(Duration.class));
  }

  @Test
  void sameCustomerIdIsSerializedAcrossThreads() throws Exception {
    String key = CustomerIdLock.lockKey(SAME_CUSTOMER_ID);
    AtomicBoolean held = new AtomicBoolean();
    AtomicReference<String> storedToken = new AtomicReference<>();
    when(valueOps.setIfAbsent(eq(key), anyString(), any(Duration.class)))
        .thenAnswer(
            invocation -> {
              if (held.compareAndSet(false, true)) {
                storedToken.set(invocation.getArgument(1));
                return true;
              }
              return false;
            });
    when(valueOps.get(key)).thenAnswer(invocation -> storedToken.get());
    when(redis.delete(key))
        .thenAnswer(
            invocation -> {
              held.set(false);
              storedToken.set(null);
              return true;
            });

    AtomicInteger inCriticalSection = new AtomicInteger();
    AtomicInteger maxConcurrent = new AtomicInteger();
    CountDownLatch start = new CountDownLatch(1);
    int workers = 8;
    try (ExecutorService pool = Executors.newVirtualThreadPerTaskExecutor()) {
      CountDownLatch done = new CountDownLatch(workers);
      for (int i = 0; i < workers; i++) {
        pool.submit(
            () -> {
              try {
                start.await();
                lock.acquire(SAME_CUSTOMER_ID)
                    .ifPresent(
                        lease -> {
                          try (lease) {
                            int now = inCriticalSection.incrementAndGet();
                            maxConcurrent.accumulateAndGet(now, Math::max);
                            inCriticalSection.decrementAndGet();
                          }
                        });
              } catch (InterruptedException _) {
                Thread.currentThread().interrupt();
              } finally {
                done.countDown();
              }
            });
      }
      start.countDown();
      assertThat(done.await(5, TimeUnit.SECONDS)).isTrue();
    }
    assertThat(maxConcurrent.get()).isLessThanOrEqualTo(1);
  }

  private static AppProperties properties() {
    return new AppProperties(
        new AppProperties.Kafka("in", "out"),
        new AppProperties.Redis(
            AppConstants.Redis.DEFAULT_LOCK_TTL,
            LOCK_MAX_ATTEMPTS,
            Duration.ofMillis(1),
            AppConstants.Redis.DEFAULT_DEDUP_TTL),
        new AppProperties.Mongo(1, 2),
        null);
  }
}
