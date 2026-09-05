package com.acme.customeringest.producer;

import com.acme.customeringest.avro.CustomerIngestEvent;
import com.acme.customeringest.avro.CustomerProcessedEvent;
import com.acme.customeringest.avro.ProcessingStatus;
import com.acme.customeringest.config.AppProperties;
import com.acme.customeringest.tracing.KafkaRecordTracing;
import java.time.Instant;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class CustomerProcessedPublisher {

  private static final Logger LOG = LoggerFactory.getLogger(CustomerProcessedPublisher.class);

  private final KafkaTemplate<String, CustomerProcessedEvent> kafkaTemplate;
  private final KafkaRecordTracing tracing;
  private final String outboundTopic;

  public CustomerProcessedPublisher(
      KafkaTemplate<String, CustomerProcessedEvent> kafkaTemplate,
      KafkaRecordTracing tracing,
      AppProperties properties) {
    this.kafkaTemplate = kafkaTemplate;
    this.tracing = tracing;
    this.outboundTopic = properties.kafka().outboundTopic();
  }

  public void publishProcessed(
      CustomerIngestEvent ingest, String inboundTopic, int inboundPartition, long inboundOffset) {
    CustomerProcessedEvent event =
        CustomerProcessedEvent.newBuilder()
            .setCustomerId(ingest.getCustomerId())
            .setPhoneNumber(ingest.getPhoneNumber())
            .setAddress(ingest.getAddress())
            .setStatus(ProcessingStatus.PROCESSED)
            .setProcessedAt(Instant.now().toString())
            .setReason(ProcessingStatus.PROCESSED.name())
            .setInboundTopic(inboundTopic)
            .setInboundPartition(inboundPartition)
            .setInboundOffset(inboundOffset)
            .build();

    ProducerRecord<String, CustomerProcessedEvent> producerRecord =
        new ProducerRecord<>(outboundTopic, ingest.getCustomerId(), event);
    tracing.inject(producerRecord.headers());
    kafkaTemplate.send(producerRecord);
    LOG.info(
        "Published PROCESSED event customerId={} topic={} inbound={}-{}-{}",
        ingest.getCustomerId(),
        outboundTopic,
        inboundTopic,
        inboundPartition,
        inboundOffset);
  }
}
