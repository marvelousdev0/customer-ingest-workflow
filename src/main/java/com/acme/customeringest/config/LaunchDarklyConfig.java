package com.acme.customeringest.config;

import com.launchdarkly.sdk.server.LDClient;
import com.launchdarkly.sdk.server.LDConfig;
import com.launchdarkly.sdk.server.interfaces.LDClientInterface;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class LaunchDarklyConfig {

  private static final Logger LOG = LoggerFactory.getLogger(LaunchDarklyConfig.class);

  @Bean(destroyMethod = "close")
  LDClientInterface launchDarklyClient(AppProperties appProperties) {
    AppProperties.LaunchDarkly ld = appProperties.launchDarkly();
    boolean offline = ld.effectiveOffline();
    LDConfig config = new LDConfig.Builder().offline(offline).startWait(ld.startWait()).build();
    LOG.info(
        "LaunchDarkly client starting enabled={} offline={} startWait={}",
        ld.enabled(),
        offline,
        ld.startWait());
    return new LDClient(ld.sdkKey(), config);
  }
}
