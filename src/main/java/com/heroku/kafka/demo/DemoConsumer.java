package com.heroku.kafka.demo;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.Lists;
import io.dropwizard.lifecycle.Managed;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;
import java.util.Queue;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.util.Collections.singletonList;

public class DemoConsumer implements Managed {
  private static final Logger LOG = LoggerFactory.getLogger(DemoConsumer.class);

  private static final int CAPACITY = 10;

  private final KafkaConfig config;

  private final MetricRegistry metrics;

  private ExecutorService executor;

  private final AtomicBoolean running = new AtomicBoolean();

  private CountDownLatch stopLatch;

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
    running.set(true);
    stopLatch = new CountDownLatch(1);
  }

  private void loop() {
    LOG.info("starting");
    Properties properties = config.getProperties();
    properties.put(ConsumerConfig.GROUP_ID_CONFIG, "test");
    properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
    properties.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
    properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

    consumer = new KafkaConsumer<>(properties);
    consumer.subscribe(singletonList(config.getTopic()));

    consumer.metrics().forEach((name, metric) -> {
      Gauge<Double> gauge = () -> metric.value();
      metrics.register(MetricRegistry.name(DemoConsumer.class, name.name()), gauge);
    });
    LOG.info("started");

    do {
      ConsumerRecords<String, String> records = consumer.poll(100);
      for (ConsumerRecord<String, String> record : records) {
        LOG.debug("offset={}, key={}, value={}", record.offset(), record.key(), record.value());

        while (queue.size() >= CAPACITY) {
          queue.poll();
        }

        DemoMessage message = new DemoMessage(record.value(), config.getTopic(), record.partition(), record.offset());

        if (queue.offer(message)) {
          consumer.commitSync();
        } else {
          LOG.error("Failed to track message: {}", message);
        }
      }
    } while (running.get());

    LOG.info("closing consumer");
    consumer.close();
    stopLatch.countDown();
  }

  public List<DemoMessage> getMessages() {
    List<DemoMessage> messages = Lists.newArrayList();
    queue.iterator().forEachRemaining(messages::add);
    return messages;
  }

  @Override
  public void stop() throws Exception {
    LOG.info("stopping");
    running.set(false);
    stopLatch.await();
    executor.shutdown();
    LOG.info("stopped");
  }
}
