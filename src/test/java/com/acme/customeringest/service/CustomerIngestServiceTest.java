package com.acme.customeringest.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.acme.customeringest.avro.Address;
import com.acme.customeringest.avro.CustomerIngestEvent;
import com.acme.customeringest.featureflags.FeatureFlags;
import com.acme.customeringest.mongo.CustomerDocument;
import com.acme.customeringest.mongo.CustomerRepository;
import com.acme.customeringest.producer.CustomerProcessedPublisher;
import com.acme.customeringest.redis.CustomerIdLock;
import com.acme.customeringest.redis.DuplicateMessageGuard;
import java.util.Optional;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class CustomerIngestServiceTest {

  private static final String INBOUND_TOPIC = "inbound";

  @Mock private DuplicateMessageGuard duplicateMessageGuard;

  @Mock private CustomerIdLock customerIdLock;

  @Mock private CustomerRepository customerRepository;

  @Mock private CustomerProcessedPublisher publisher;

  @Mock private FeatureFlags featureFlags;

  private CustomerIngestService service;

  @BeforeEach
  void setUp() {
    service =
        new CustomerIngestService(
            duplicateMessageGuard, customerIdLock, customerRepository, publisher, featureFlags);
  }

  @Test
  void duplicateSkipsMongoAndOutboundPublish() {
    ConsumerRecord<String, CustomerIngestEvent> consumerRecord = ingestRecord("cust-1", 15);
    when(duplicateMessageGuard.alreadyProcessed(INBOUND_TOPIC, 0, 15L, null)).thenReturn(true);

    ProcessingOutcome outcome = service.process(consumerRecord);

    assertThat(outcome).isEqualTo(ProcessingOutcome.DUPLICATE_SKIPPED);
    verify(customerRepository, never()).save(any());
    verify(publisher, never()).publishProcessed(any(), anyString(), anyInt(), anyLong());
    verify(customerIdLock, never()).acquire(anyString());
  }

  @Test
  void successWritesMongoAndPublishes() {
    ConsumerRecord<String, CustomerIngestEvent> consumerRecord = ingestRecord("cust-2", 21);
    when(duplicateMessageGuard.alreadyProcessed(INBOUND_TOPIC, 0, 21L, null)).thenReturn(false);
    when(customerIdLock.acquire("cust-2"))
        .thenReturn(Optional.of(mock(CustomerIdLock.LockLease.class)));
    when(customerRepository.findById("cust-2")).thenReturn(Optional.empty());
    when(customerRepository.save(any(CustomerDocument.class)))
        .thenAnswer(invocation -> invocation.getArgument(0));
    when(featureFlags.isOutboundPublishEnabled("cust-2")).thenReturn(true);

    ProcessingOutcome outcome = service.process(consumerRecord);

    assertThat(outcome).isEqualTo(ProcessingOutcome.PROCESSED);
    verify(customerRepository).save(any(CustomerDocument.class));
    verify(duplicateMessageGuard).markProcessed(INBOUND_TOPIC, 0, 21L, null);
    verify(publisher).publishProcessed(any(), eq(INBOUND_TOPIC), eq(0), eq(21L));
  }

  @Test
  void successWritesMongoButSkipsPublishWhenFlagOff() {
    ConsumerRecord<String, CustomerIngestEvent> consumerRecord = ingestRecord("cust-4", 33);
    when(duplicateMessageGuard.alreadyProcessed(INBOUND_TOPIC, 0, 33L, null)).thenReturn(false);
    when(customerIdLock.acquire("cust-4"))
        .thenReturn(Optional.of(mock(CustomerIdLock.LockLease.class)));
    when(customerRepository.findById("cust-4")).thenReturn(Optional.empty());
    when(customerRepository.save(any(CustomerDocument.class)))
        .thenAnswer(invocation -> invocation.getArgument(0));
    when(featureFlags.isOutboundPublishEnabled("cust-4")).thenReturn(false);

    ProcessingOutcome outcome = service.process(consumerRecord);

    assertThat(outcome).isEqualTo(ProcessingOutcome.PROCESSED);
    verify(customerRepository).save(any(CustomerDocument.class));
    verify(duplicateMessageGuard).markProcessed(INBOUND_TOPIC, 0, 33L, null);
    verify(publisher, never()).publishProcessed(any(), anyString(), anyInt(), anyLong());
  }

  @Test
  void missingRequiredFieldsFailsValidation() {
    CustomerIngestEvent event =
        CustomerIngestEvent.newBuilder()
            .setCustomerId("")
            .setPhoneNumber("555-0100")
            .setAddress(
                Address.newBuilder()
                    .setStreet("1 Main")
                    .setCity("Austin")
                    .setState("TX")
                    .setZip("78701")
                    .build())
            .build();
    ConsumerRecord<String, CustomerIngestEvent> consumerRecord =
        new ConsumerRecord<>(INBOUND_TOPIC, 0, 1L, "cust", event);

    assertThrows(IllegalArgumentException.class, () -> service.process(consumerRecord));
    verify(customerRepository, never()).save(any());
    verify(publisher, never()).publishProcessed(any(), anyString(), anyInt(), anyLong());
  }

  @Test
  void lockFailureDoesNotWriteOrPublish() {
    ConsumerRecord<String, CustomerIngestEvent> consumerRecord = ingestRecord("cust-3", 9);
    when(duplicateMessageGuard.alreadyProcessed(INBOUND_TOPIC, 0, 9L, null)).thenReturn(false);
    when(customerIdLock.acquire("cust-3")).thenReturn(Optional.empty());

    ProcessingOutcome outcome = service.process(consumerRecord);

    assertThat(outcome).isEqualTo(ProcessingOutcome.LOCK_NOT_ACQUIRED);
    verify(customerRepository, never()).save(any());
    verify(publisher, never()).publishProcessed(any(), anyString(), anyInt(), anyLong());
    verify(duplicateMessageGuard, never()).markProcessed(anyString(), anyInt(), anyLong(), any());
  }

  private static ConsumerRecord<String, CustomerIngestEvent> ingestRecord(
      String customerId, long offset) {
    CustomerIngestEvent event =
        CustomerIngestEvent.newBuilder()
            .setCustomerId(customerId)
            .setPhoneNumber("555-0100")
            .setAddress(
                Address.newBuilder()
                    .setStreet("1 Main")
                    .setCity("Austin")
                    .setState("TX")
                    .setZip("78701")
                    .build())
            .build();
    return new ConsumerRecord<>(INBOUND_TOPIC, 0, offset, customerId, event);
  }
}
