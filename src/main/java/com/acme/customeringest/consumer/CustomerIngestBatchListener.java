package com.acme.customeringest.consumer;

import com.acme.customeringest.avro.CustomerIngestEvent;
import com.acme.customeringest.common.AppConstants;
import com.acme.customeringest.service.CustomerIngestService;
import com.acme.customeringest.service.ProcessingOutcome;
import com.acme.customeringest.tracing.KafkaRecordTracing;
import java.util.List;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

@Component
public class CustomerIngestBatchListener {

  private static final Logger LOG = LoggerFactory.getLogger(CustomerIngestBatchListener.class);

  private final CustomerIngestService ingestService;
  private final KafkaRecordTracing tracing;

  public CustomerIngestBatchListener(
      CustomerIngestService ingestService, KafkaRecordTracing tracing) {
    this.ingestService = ingestService;
    this.tracing = tracing;
  }

  @KafkaListener(
      topics = "${app.kafka.inbound-topic}",
      groupId = "${spring.kafka.consumer.group-id}",
      containerFactory = AppConstants.Kafka.LISTENER_CONTAINER_FACTORY,
      batch = "true")
  public void onBatch(
      List<ConsumerRecord<String, CustomerIngestEvent>> records, Acknowledgment acknowledgment) {
    LOG.info("Consumed batch size={}", records.size());
    Integer firstUnacked = null;
    for (int i = 0; i < records.size(); i++) {
      ConsumerRecord<String, CustomerIngestEvent> consumerRecord = records.get(i);
      ProcessingOutcome outcome = processRecord(consumerRecord);
      if (needsRetry(outcome) && firstUnacked == null) {
        firstUnacked = i;
      }
    }
    if (firstUnacked == null) {
      acknowledgment.acknowledge();
      LOG.info("Acked full batch size={}", records.size());
    } else {
      // Contiguous commit up to the first lock/failure. Later successes may be
      // redelivered; Redis dedup makes that safe. Other customerIds in the batch
      // were still processed before this nack.
      acknowledgment.nack(firstUnacked, AppConstants.Kafka.NACK_SLEEP);
      LOG.warn("Nacked batch from index={} size={}", firstUnacked, records.size());
    }
  }

  private ProcessingOutcome processRecord(
      ConsumerRecord<String, CustomerIngestEvent> consumerRecord) {
    try (var _ = tracing.open(consumerRecord)) {
      return ingestService.process(consumerRecord);
    } catch (RuntimeException ex) {
      LOG.warn(
          "Unhandled listener error {}-{}-{}: {}",
          consumerRecord.topic(),
          consumerRecord.partition(),
          consumerRecord.offset(),
          ex.toString());
      return ProcessingOutcome.FAILED;
    }
  }

  private static boolean needsRetry(ProcessingOutcome outcome) {
    return switch (outcome) {
      case LOCK_NOT_ACQUIRED, FAILED -> true;
      case PROCESSED, DUPLICATE_SKIPPED -> false;
    };
  }
}
