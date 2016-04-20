package com.heroku.kafka.demo;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.Lists;
import io.dropwizard.lifecycle.Managed;
import io.dropwizard.metrics.MetricsFactory;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static java.util.Collections.singletonList;

public class DemoConsumer implements Managed {
  private static final Logger LOG = LoggerFactory.getLogger(DemoConsumer.class);

  private static final int CAPACITY = 10;

  private final KafkaConfig config;

  private final MetricRegistry metrics;

  private ExecutorService executor;

  private KafkaConsumer<String, String> consumer;

  private final Queue<DemoMessage> queue = new ArrayBlockingQueue<>(CAPACITY);

  public DemoConsumer(KafkaConfig config, MetricRegistry metrics) {
    this.config = config;
    this.metrics = metrics;
  }

  @Override
  public void start() throws Exception {
    executor = Executors.newSingleThreadExecutor();
    executor.submit(this::loop);
  }

  private void loop() {
    Properties properties = config.getProperties();
    properties.put(ConsumerConfig.GROUP_ID_CONFIG, "test");
    properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
    properties.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
    properties.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
    properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

    consumer = new KafkaConsumer<>(properties);
    consumer.subscribe(singletonList(config.getTopic()));

    consumer.metrics().forEach((name, metric) -> {
      Gauge<Double> gauge = () -> metric.value();
      metrics.register(MetricRegistry.name(DemoConsumer.class, name.name()), gauge);
    });

    do {
      ConsumerRecords<String, String> records = consumer.poll(100);
      for (ConsumerRecord<String, String> record : records) {
        LOG.debug("offset={}, key={}, value={}", record.offset(), record.key(), record.value());

        while (queue.size() >= CAPACITY) {
          queue.poll();
        }

        DemoMessage message = new DemoMessage(record.partition(), record.offset(), record.value(), new DateTime());

        if (!queue.offer(message)) {
          LOG.error("failed to track record");
        }
      }
    } while (true);
  }

  public List<DemoMessage> getMessages() {
    List<DemoMessage> messages = Lists.newArrayList();
    queue.iterator().forEachRemaining(messages::add);
    return messages;
  }

  @Override
  public void stop() throws Exception {
    // TODO: notify shutdown
    consumer.close();
    executor.shutdown();
  }
}
