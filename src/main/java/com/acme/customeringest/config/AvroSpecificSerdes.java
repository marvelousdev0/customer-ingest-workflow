package com.acme.customeringest.config;

import com.acme.customeringest.avro.CustomerIngestEvent;
import com.acme.customeringest.avro.CustomerProcessedEvent;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import java.util.Map;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;

/**
 * Isolates the Confluent Avro {@code SpecificRecord} casts required because {@link
 * KafkaAvroDeserializer} and {@link KafkaAvroSerializer} are typed as {@code Object} rather than
 * the generated event classes.
 */
final class AvroSpecificSerdes {

  private static final boolean VALUE_SERDE = false;

  private AvroSpecificSerdes() {
    throw new AssertionError("Utility class");
  }

  @SuppressWarnings({"unchecked", "java:S3740"})
  static Deserializer<CustomerIngestEvent> consumerValueDeserializer(Map<String, Object> props) {
    KafkaAvroDeserializer deserializer = new KafkaAvroDeserializer();
    deserializer.configure(props, VALUE_SERDE);
    return (Deserializer<CustomerIngestEvent>) (Deserializer<?>) deserializer;
  }

  @SuppressWarnings({"unchecked", "java:S3740"})
  static Serializer<CustomerProcessedEvent> producerValueSerializer(Map<String, Object> props) {
    KafkaAvroSerializer serializer = new KafkaAvroSerializer();
    serializer.configure(props, VALUE_SERDE);
    return (Serializer<CustomerProcessedEvent>) (Serializer<?>) serializer;
  }
}
