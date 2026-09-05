package com.acme.customeringest.config;

import com.acme.customeringest.avro.CustomerProcessedEvent;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.boot.kafka.autoconfigure.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

/**
 * Outbound values are generated {@link CustomerProcessedEvent} SpecificRecords serialized with
 * Confluent {@link KafkaAvroSerializer}.
 */
@Configuration
public class KafkaProducerConfig {

  @Bean
  ProducerFactory<String, CustomerProcessedEvent> producerFactory(KafkaProperties kafkaProperties) {
    Map<String, Object> props = new HashMap<>(kafkaProperties.buildProducerProperties());
    return new DefaultKafkaProducerFactory<>(
        props, new StringSerializer(), AvroSpecificSerdes.producerValueSerializer(props));
  }

  @Bean
  KafkaTemplate<String, CustomerProcessedEvent> kafkaTemplate(
      ProducerFactory<String, CustomerProcessedEvent> producerFactory) {
    KafkaTemplate<String, CustomerProcessedEvent> template = new KafkaTemplate<>(producerFactory);
    template.setObservationEnabled(true);
    return template;
  }
}
