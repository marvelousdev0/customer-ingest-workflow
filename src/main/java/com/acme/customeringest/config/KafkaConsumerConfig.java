package com.acme.customeringest.config;

import com.acme.customeringest.avro.CustomerIngestEvent;
import com.acme.customeringest.common.AppConstants;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.boot.kafka.autoconfigure.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;

/**
 * Inbound values are generated {@link CustomerIngestEvent} SpecificRecords. {@code
 * specific.avro.reader=true} so KafkaAvroDeserializer never returns GenericRecord.
 */
@Configuration
@EnableKafka
public class KafkaConsumerConfig {

  @Bean
  ConsumerFactory<String, CustomerIngestEvent> consumerFactory(KafkaProperties kafkaProperties) {
    Map<String, Object> props = new HashMap<>(kafkaProperties.buildConsumerProperties());
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, AppConstants.Kafka.ENABLE_AUTO_COMMIT);
    props.putIfAbsent(
        ConsumerConfig.MAX_POLL_RECORDS_CONFIG, AppConstants.Kafka.DEFAULT_MAX_POLL_RECORDS);
    props.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true);

    return new DefaultKafkaConsumerFactory<>(
        props,
        new StringDeserializer(),
        new ErrorHandlingDeserializer<>(AvroSpecificSerdes.consumerValueDeserializer(props)));
  }

  @Bean
  ConcurrentKafkaListenerContainerFactory<String, CustomerIngestEvent>
      kafkaListenerContainerFactory(
          ConsumerFactory<String, CustomerIngestEvent> consumerFactory,
          KafkaProperties kafkaProperties) {
    ConcurrentKafkaListenerContainerFactory<String, CustomerIngestEvent> factory =
        new ConcurrentKafkaListenerContainerFactory<>();
    factory.setConsumerFactory(consumerFactory);
    factory.setBatchListener(true);
    Integer configuredConcurrency = kafkaProperties.getListener().getConcurrency();
    int concurrency =
        configuredConcurrency != null
            ? configuredConcurrency
            : AppConstants.Kafka.DEFAULT_LISTENER_CONCURRENCY;
    factory.setConcurrency(concurrency);
    factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);
    factory.getContainerProperties().setObservationEnabled(true);
    return factory;
  }
}
