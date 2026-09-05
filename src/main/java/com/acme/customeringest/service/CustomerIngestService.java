package com.acme.customeringest.service;

import com.acme.customeringest.avro.Address;
import com.acme.customeringest.avro.CustomerIngestEvent;
import com.acme.customeringest.common.AppConstants;
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

  public CustomerIngestService(
      DuplicateMessageGuard duplicateMessageGuard,
      CustomerIdLock customerIdLock,
      CustomerRepository customerRepository,
      CustomerProcessedPublisher publisher) {
    this.duplicateMessageGuard = duplicateMessageGuard;
    this.customerIdLock = customerIdLock;
    this.customerRepository = customerRepository;
    this.publisher = publisher;
  }

  public ProcessingOutcome process(ConsumerRecord<String, CustomerIngestEvent> record) {
    CustomerIngestEvent event = record.value();
    if (event == null) {
      LOG.warn(
          "Skipping null payload {}-{}-{}", record.topic(), record.partition(), record.offset());
      return ProcessingOutcome.FAILED;
    }
    validate(event);

    String idempotencyKey = header(record, AppConstants.Redis.IDEMPOTENCY_HEADER);
    LOG.info(
        "Processing ingest customerId={} {}-{}-{}",
        event.getCustomerId(),
        record.topic(),
        record.partition(),
        record.offset());
    logPii(event);

    if (duplicateMessageGuard.alreadyProcessed(
        record.topic(), record.partition(), record.offset(), idempotencyKey)) {
      return ProcessingOutcome.DUPLICATE_SKIPPED;
    }

    return processWithLock(event, record, idempotencyKey);
  }

  private ProcessingOutcome processWithLock(
      CustomerIngestEvent event,
      ConsumerRecord<String, CustomerIngestEvent> record,
      String idempotencyKey) {
    Optional<CustomerIdLock.LockLease> maybeLease = customerIdLock.acquire(event.getCustomerId());
    if (maybeLease.isEmpty()) {
      return ProcessingOutcome.LOCK_NOT_ACQUIRED;
    }

    try (CustomerIdLock.LockLease ignored = maybeLease.orElseThrow()) {
      persist(event, record);
      duplicateMessageGuard.markProcessed(
          record.topic(), record.partition(), record.offset(), idempotencyKey);
      publisher.publishProcessed(event, record.topic(), record.partition(), record.offset());
      LOG.info(
          "Processed customerId={} {}-{}-{}",
          event.getCustomerId(),
          record.topic(),
          record.partition(),
          record.offset());
      return ProcessingOutcome.PROCESSED;
    } catch (RuntimeException ex) {
      LOG.warn(
          "Failed processing customerId={} {}-{}-{}: {}",
          event.getCustomerId(),
          record.topic(),
          record.partition(),
          record.offset(),
          ex.toString());
      return ProcessingOutcome.FAILED;
    }
  }

  private void persist(
      CustomerIngestEvent event, ConsumerRecord<String, CustomerIngestEvent> record) {
    Address address = event.getAddress();
    CustomerDocument document =
        customerRepository.findById(event.getCustomerId()).orElseGet(CustomerDocument::new);
    document.setCustomerId(event.getCustomerId());
    document.setPhoneNumber(event.getPhoneNumber());
    document.setAddress(
        new AddressEmbed(
            address.getStreet(), address.getCity(), address.getState(), address.getZip()));
    document.setUpdatedAt(Instant.now());
    document.setLastInboundTopic(record.topic());
    document.setLastInboundPartition(record.partition());
    document.setLastInboundOffset(record.offset());
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

  private static String header(ConsumerRecord<?, ?> record, String name) {
    Header header = record.headers().lastHeader(name);
    if (header == null || header.value() == null) {
      return null;
    }
    return new String(header.value(), StandardCharsets.UTF_8);
  }
}
