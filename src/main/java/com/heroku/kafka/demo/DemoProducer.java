package com.heroku.kafka.demo;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import io.dropwizard.lifecycle.Managed;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.Future;

public class DemoProducer implements Managed {
  private final KafkaConfig config;

  private final MetricRegistry metrics;

  private Producer<String, String> producer;

  public DemoProducer(KafkaConfig config, MetricRegistry metrics) {
    this.config = config;
    this.metrics = metrics;
  }

  public void start() throws Exception {
    Properties properties = config.getProperties();
    properties.put(ProducerConfig.ACKS_CONFIG, "all");
    properties.put(ProducerConfig.RETRIES_CONFIG, 0);
    properties.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
    properties.put(ProducerConfig.LINGER_MS_CONFIG, 1);
    properties.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
    properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

    producer = new KafkaProducer<>(properties);

    producer.metrics().forEach((name, metric) -> {
      Gauge<Double> gauge = () -> metric.value();
      metrics.register(MetricRegistry.name(DemoProducer.class, name.name()), gauge);
    });

  }

  public Future<RecordMetadata> send(String message) {
    return producer.send(new ProducerRecord<>(config.getTopic(), message, message));
  }

  public void stop() throws Exception {
    Producer<String, String> producer = this.producer;
    this.producer = null;
    producer.close();
  }
}
