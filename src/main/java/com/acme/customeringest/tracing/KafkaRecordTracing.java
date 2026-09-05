package com.acme.customeringest.tracing;

import com.acme.customeringest.common.AppConstants;
import io.micrometer.tracing.Span;
import io.micrometer.tracing.Tracer;
import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import io.opentelemetry.context.propagation.TextMapPropagator;
import io.opentelemetry.extension.trace.propagation.B3Propagator;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Headers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * Extracts W3C {@code traceparent} / B3 from inbound Kafka headers, opens a child span so MDC
 * {@code traceId}/{@code spanId} stay stable through dedup → lock → mongo → produce, and injects
 * the same context onto outbound records.
 */
@Component
public class KafkaRecordTracing {

  private static final Logger LOG = LoggerFactory.getLogger(KafkaRecordTracing.class);

  private final Tracer tracer;
  private final TextMapPropagator propagator;

  public KafkaRecordTracing(Tracer tracer) {
    this.tracer = tracer;
    this.propagator =
        TextMapPropagator.composite(
            W3CTraceContextPropagator.getInstance(),
            B3Propagator.injectingMultiHeaders(),
            B3Propagator.injectingSingleHeader());
  }

  public TraceScope open(ConsumerRecord<?, ?> consumerRecord) {
    Context extracted =
        propagator.extract(Context.current(), consumerRecord.headers(), KafkaHeaderTextMap.GETTER);
    Scope otelScope = extracted.makeCurrent();
    Span span =
        tracer
            .nextSpan()
            .name(AppConstants.Tracing.SPAN_NAME)
            .tag(
                AppConstants.Tracing.TAG_MESSAGING_SYSTEM,
                AppConstants.Tracing.MESSAGING_SYSTEM_KAFKA)
            .tag(AppConstants.Tracing.TAG_MESSAGING_DESTINATION, consumerRecord.topic())
            .tag(
                AppConstants.Tracing.TAG_MESSAGING_PARTITION,
                String.valueOf(consumerRecord.partition()))
            .tag(AppConstants.Tracing.TAG_MESSAGING_OFFSET, String.valueOf(consumerRecord.offset()))
            .start();
    Tracer.SpanInScope micrometerScope = tracer.withSpan(span);
    LOG.debug(
        "Opened ingest span for {}-{}-{}",
        consumerRecord.topic(),
        consumerRecord.partition(),
        consumerRecord.offset());
    return new TraceScope(span, micrometerScope, otelScope);
  }

  public void inject(Headers headers) {
    propagator.inject(Context.current(), headers, KafkaHeaderTextMap.SETTER);
  }

  public static final class TraceScope implements AutoCloseable {

    private final Span span;
    private final Tracer.SpanInScope micrometerScope;
    private final Scope otelScope;

    TraceScope(Span span, Tracer.SpanInScope micrometerScope, Scope otelScope) {
      this.span = span;
      this.micrometerScope = micrometerScope;
      this.otelScope = otelScope;
    }

    @Override
    public void close() {
      span.end();
      micrometerScope.close();
      otelScope.close();
    }
  }
}
