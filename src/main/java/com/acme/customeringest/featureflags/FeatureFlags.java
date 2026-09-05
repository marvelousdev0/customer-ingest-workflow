package com.acme.customeringest.featureflags;

import com.acme.customeringest.common.AppConstants;
import com.launchdarkly.sdk.LDContext;
import com.launchdarkly.sdk.server.interfaces.LDClientInterface;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

/**
 * Thin LaunchDarkly wrapper so call sites depend on app flag names, not the SDK.
 *
 * <p>Secrets (SDK key) come from Vault / env; flag definitions live in the LaunchDarkly dashboard.
 */
@Component
public class FeatureFlags {

  private final LDClientInterface client;
  private final String applicationKey;

  public FeatureFlags(LDClientInterface client, Environment env) {
    this.client = client;
    String name = env.getProperty("spring.application.name");
    this.applicationKey = StringUtils.hasText(name) ? name : "customer-ingest-workflow";
  }

  /** Application-scoped evaluation (no per-customer targeting). */
  public boolean isEnabled(String flagKey, boolean defaultValue) {
    return client.boolVariation(flagKey, applicationContext(), defaultValue);
  }

  /** Customer-scoped evaluation for percentage rollouts / targeting. */
  public boolean isEnabledForCustomer(String flagKey, String customerId, boolean defaultValue) {
    return client.boolVariation(flagKey, customerContext(customerId), defaultValue);
  }

  public boolean isOutboundPublishEnabled(String customerId) {
    return isEnabledForCustomer(
        AppConstants.FeatureFlags.OUTBOUND_PUBLISH,
        customerId,
        AppConstants.FeatureFlags.DEFAULT_OUTBOUND_PUBLISH);
  }

  private LDContext applicationContext() {
    return LDContext.builder(applicationKey)
        .kind(AppConstants.FeatureFlags.CONTEXT_KIND_APPLICATION)
        .build();
  }

  private static LDContext customerContext(String customerId) {
    String key = StringUtils.hasText(customerId) ? customerId : "anonymous";
    return LDContext.builder(key)
        .kind(AppConstants.FeatureFlags.CONTEXT_KIND_CUSTOMER)
        .set(AppConstants.FeatureFlags.ATTR_CUSTOMER_ID, key)
        .build();
  }
}
