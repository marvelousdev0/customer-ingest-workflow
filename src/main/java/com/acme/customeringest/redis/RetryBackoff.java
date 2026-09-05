package com.acme.customeringest.redis;

import java.time.Duration;
import java.util.concurrent.locks.LockSupport;

/**
 * Documented retry wait used only when a per-customer Redis lock is contended. Uses {@link
 * LockSupport#parkNanos(long)} instead of {@code Thread.sleep} so the wait is interrupt-aware and
 * stays off the allocation-heavy happy path.
 */
public final class RetryBackoff {

  private RetryBackoff() {
    throw new AssertionError("Utility class");
  }

  public static void park(Duration duration) {
    if (duration == null || duration.isNegative() || duration.isZero()) {
      return;
    }
    LockSupport.parkNanos(duration.toNanos());
  }
}
