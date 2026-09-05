package com.acme.customeringest.config;

import com.acme.customeringest.common.AppConstants;
import com.mongodb.connection.ConnectionPoolSettings;
import java.util.concurrent.TimeUnit;
import org.springframework.boot.mongodb.autoconfigure.MongoClientSettingsBuilderCustomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.mongodb.repository.config.EnableMongoRepositories;

@Configuration
@EnableMongoRepositories(basePackages = "com.acme.customeringest.mongo")
public class MongoConfig {

  @Bean
  MongoClientSettingsBuilderCustomizer mongoPoolCustomizer(AppProperties properties) {
    AppProperties.Mongo mongo = properties.mongo();
    return builder ->
        builder.applyToConnectionPoolSettings(
            pool ->
                pool.applySettings(
                    ConnectionPoolSettings.builder()
                        .minSize(mongo.connectionPoolMin())
                        .maxSize(mongo.connectionPoolMax())
                        .maxWaitTime(AppConstants.Mongo.MAX_WAIT_TIME_SECONDS, TimeUnit.SECONDS)
                        .maxConnectionIdleTime(
                            AppConstants.Mongo.MAX_IDLE_TIME_SECONDS, TimeUnit.SECONDS)
                        .build()));
  }
}
