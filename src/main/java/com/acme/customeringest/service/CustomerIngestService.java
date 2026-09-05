package com.acme.customeringest.service;

import com.acme.customeringest.avro.Address;
import com.acme.customeringest.avro.CustomerIngestEvent;
import com.acme.customeringest.common.AppConstants;
import com.acme.customeringest.featureflags.FeatureFlags;
import com.acme.customeringest.mongo.AddressEmbed;
import com.acme.customeringest.mongo.CustomerDocument;
import com.acme.customeringest.mongo.CustomerRepository;
import com.acme.customeringest.producer.CustomerProcessedPublisher;
import com.acme.customeringest.redis.CustomerIdLock;
import com.acme.customeringest.redis.DuplicateMessageGuard;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Optional;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

@Service
public class CustomerIngestService {

  private static final Logger LOG = LoggerFactory.getLogger(CustomerIngestService.class);

  private final DuplicateMessageGuard duplicateMessageGuard;
  private final CustomerIdLock customerIdLock;
  private final CustomerRepository customerRepository;
  private final CustomerProcessedPublisher publisher;
  private final FeatureFlags featureFlags;

  public CustomerIngestService(
      DuplicateMessageGuard duplicateMessageGuard,
      CustomerIdLock customerIdLock,
      CustomerRepository customerRepository,
      CustomerProcessedPublisher publisher,
      FeatureFlags featureFlags) {
    this.duplicateMessageGuard = duplicateMessageGuard;
    this.customerIdLock = customerIdLock;
    this.customerRepository = customerRepository;
    this.publisher = publisher;
    this.featureFlags = featureFlags;
  }

  public ProcessingOutcome process(ConsumerRecord<String, CustomerIngestEvent> consumerRecord) {
    CustomerIngestEvent event = consumerRecord.value();
    if (event == null) {
      LOG.warn(
          "Skipping null payload {}-{}-{}",
          consumerRecord.topic(),
          consumerRecord.partition(),
          consumerRecord.offset());
      return ProcessingOutcome.FAILED;
    }
    validate(event);

    String idempotencyKey = header(consumerRecord, AppConstants.Redis.IDEMPOTENCY_HEADER);
    LOG.info(
        "Processing ingest customerId={} {}-{}-{}",
        event.getCustomerId(),
        consumerRecord.topic(),
        consumerRecord.partition(),
        consumerRecord.offset());
    logPii(event);

    if (duplicateMessageGuard.alreadyProcessed(
        consumerRecord.topic(),
        consumerRecord.partition(),
        consumerRecord.offset(),
        idempotencyKey)) {
      return ProcessingOutcome.DUPLICATE_SKIPPED;
    }

    return processWithLock(event, consumerRecord, idempotencyKey);
  }

  private ProcessingOutcome processWithLock(
      CustomerIngestEvent event,
      ConsumerRecord<String, CustomerIngestEvent> consumerRecord,
      String idempotencyKey) {
    Optional<CustomerIdLock.LockLease> maybeLease = customerIdLock.acquire(event.getCustomerId());
    if (maybeLease.isEmpty()) {
      return ProcessingOutcome.LOCK_NOT_ACQUIRED;
    }

    try (var _ = maybeLease.orElseThrow()) {
      persist(event, consumerRecord);
      duplicateMessageGuard.markProcessed(
          consumerRecord.topic(),
          consumerRecord.partition(),
          consumerRecord.offset(),
          idempotencyKey);
      if (featureFlags.isOutboundPublishEnabled(event.getCustomerId())) {
        publisher.publishProcessed(
            event, consumerRecord.topic(), consumerRecord.partition(), consumerRecord.offset());
      } else {
        LOG.info(
            "Outbound publish disabled by feature flag customerId={} flag={}",
            event.getCustomerId(),
            AppConstants.FeatureFlags.OUTBOUND_PUBLISH);
      }
      LOG.info(
          "Processed customerId={} {}-{}-{}",
          event.getCustomerId(),
          consumerRecord.topic(),
          consumerRecord.partition(),
          consumerRecord.offset());
      return ProcessingOutcome.PROCESSED;
    } catch (RuntimeException ex) {
      LOG.warn(
          "Failed processing customerId={} {}-{}-{}: {}",
          event.getCustomerId(),
          consumerRecord.topic(),
          consumerRecord.partition(),
          consumerRecord.offset(),
          ex.toString());
      return ProcessingOutcome.FAILED;
    }
  }

  private void persist(
      CustomerIngestEvent event, ConsumerRecord<String, CustomerIngestEvent> consumerRecord) {
    Address address = event.getAddress();
    CustomerDocument document =
        customerRepository.findById(event.getCustomerId()).orElseGet(CustomerDocument::new);
    document.setCustomerId(event.getCustomerId());
    document.setPhoneNumber(event.getPhoneNumber());
    document.setAddress(
        new AddressEmbed(
            address.getStreet(), address.getCity(), address.getState(), address.getZip()));
    document.setUpdatedAt(Instant.now());
    document.setLastInboundTopic(consumerRecord.topic());
    document.setLastInboundPartition(consumerRecord.partition());
    document.setLastInboundOffset(consumerRecord.offset());
    customerRepository.save(document);
    LOG.info("Wrote Mongo customer document customerId={}", event.getCustomerId());
  }

  private static void validate(CustomerIngestEvent event) {
    if (!StringUtils.hasText(event.getCustomerId())
        || !StringUtils.hasText(event.getPhoneNumber())
        || event.getAddress() == null) {
      throw new IllegalArgumentException(
          "CustomerIngestEvent missing customerId, phoneNumber, or address");
    }
  }

  private static void logPii(CustomerIngestEvent event) {
    Address address = event.getAddress();
    LOG.debug(
        "PII fields customerId={} phoneNumber={} street={} city={} state={} zip={}",
        event.getCustomerId(),
        event.getPhoneNumber(),
        address.getStreet(),
        address.getCity(),
        address.getState(),
        address.getZip());
  }

  private static String header(ConsumerRecord<?, ?> consumerRecord, String name) {
    Header header = consumerRecord.headers().lastHeader(name);
    if (header == null || header.value() == null) {
      return null;
    }
    return new String(header.value(), StandardCharsets.UTF_8);
  }
}
