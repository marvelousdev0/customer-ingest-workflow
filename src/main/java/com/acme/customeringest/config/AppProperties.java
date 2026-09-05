package com.acme.customeringest.config;

import com.acme.customeringest.common.AppConstants;
import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.util.StringUtils;

@ConfigurationProperties(prefix = "app")
public record AppProperties(Kafka kafka, Redis redis, Mongo mongo, LaunchDarkly launchDarkly) {

  public AppProperties(Kafka kafka, Redis redis, Mongo mongo, LaunchDarkly launchDarkly) {
    this.kafka = kafka != null ? kafka : Kafka.defaults();
    this.redis = redis != null ? redis : Redis.defaults();
    this.mongo = mongo != null ? mongo : Mongo.defaults();
    this.launchDarkly = launchDarkly != null ? launchDarkly : LaunchDarkly.defaults();
  }

  public record Kafka(String inboundTopic, String outboundTopic) {

    public static Kafka defaults() {
      return new Kafka(
          AppConstants.Kafka.DEFAULT_INBOUND_TOPIC, AppConstants.Kafka.DEFAULT_OUTBOUND_TOPIC);
    }
  }

  public record Redis(
      Duration lockTtl, int lockMaxAttempts, Duration lockBackoff, Duration dedupTtl) {

    public static Redis defaults() {
      return new Redis(
          AppConstants.Redis.DEFAULT_LOCK_TTL,
          AppConstants.Redis.DEFAULT_LOCK_MAX_ATTEMPTS,
          AppConstants.Redis.DEFAULT_LOCK_BACKOFF,
          AppConstants.Redis.DEFAULT_DEDUP_TTL);
    }
  }

  public record Mongo(int connectionPoolMin, int connectionPoolMax) {

    public static Mongo defaults() {
      return new Mongo(AppConstants.Mongo.DEFAULT_POOL_MIN, AppConstants.Mongo.DEFAULT_POOL_MAX);
    }
  }

  public record LaunchDarkly(boolean enabled, String sdkKey, boolean offline, Duration startWait) {

    public LaunchDarkly {
      if (!StringUtils.hasText(sdkKey)) {
        sdkKey = AppConstants.FeatureFlags.PLACEHOLDER_SDK_KEY;
      }
      if (startWait == null) {
        startWait = AppConstants.FeatureFlags.DEFAULT_START_WAIT;
      }
    }

    public static LaunchDarkly defaults() {
      return new LaunchDarkly(
          true,
          AppConstants.FeatureFlags.PLACEHOLDER_SDK_KEY,
          false,
          AppConstants.FeatureFlags.DEFAULT_START_WAIT);
    }

    /** Offline when explicitly requested, disabled, or still on the placeholder SDK key. */
    public boolean effectiveOffline() {
      return offline || !enabled || AppConstants.FeatureFlags.PLACEHOLDER_SDK_KEY.equals(sdkKey);
    }
  }
}
