package com.acme.customeringest;

import com.acme.customeringest.config.AppProperties;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

@SpringBootApplication
@EnableConfigurationProperties(AppProperties.class)
@SuppressWarnings("java:S1118") // @Configuration classes need a public constructor for Spring CGLIB
public class CustomerIngestWorkflowApplication {

  public static void main(String[] args) {
    SpringApplication.run(CustomerIngestWorkflowApplication.class, args);
  }
}
