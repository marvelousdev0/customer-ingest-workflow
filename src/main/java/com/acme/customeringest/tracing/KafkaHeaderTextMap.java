package com.acme.customeringest.tracing;

import io.opentelemetry.context.propagation.TextMapGetter;
import io.opentelemetry.context.propagation.TextMapSetter;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;

public final class KafkaHeaderTextMap {

  public static final TextMapGetter<Headers> GETTER =
      new TextMapGetter<>() {
        @Override
        public Iterable<String> keys(Headers carrier) {
          if (carrier == null) {
            return List.of();
          }
          List<String> keys = new ArrayList<>();
          for (Header header : carrier) {
            keys.add(header.key());
          }
          return keys;
        }

        @Override
        public String get(Headers carrier, String key) {
          if (carrier == null) {
            return null;
          }
          Header header = carrier.lastHeader(key);
          if (header == null || header.value() == null) {
            return null;
          }
          return new String(header.value(), StandardCharsets.UTF_8);
        }
      };

  public static final TextMapSetter<Headers> SETTER =
      (carrier, key, value) -> {
        if (carrier == null || key == null || value == null) {
          return;
        }
        carrier.remove(key);
        carrier.add(key, value.getBytes(StandardCharsets.UTF_8));
      };

  private KafkaHeaderTextMap() {
    throw new AssertionError("Utility class");
  }
}
