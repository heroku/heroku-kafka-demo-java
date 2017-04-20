package com.heroku.kafka.demo;

import io.dropwizard.lifecycle.Managed;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.Future;

public class DemoProducer implements Managed {
  private static final Logger LOG = LoggerFactory.getLogger(DemoProducer.class);

  private final KafkaConfig config;

  private Producer<String, String> producer;

  public DemoProducer(KafkaConfig config) {
    this.config = config;
  }

  public void start() throws Exception {
    LOG.info("starting");
    Properties properties = config.getProperties();

    /*
      There are several configuration properties that will make an application more robust in the face of transient
      failures, such as connectivity issues, broker restarts, and leader elections.

      We strongly recommend considering these properties when designing your application, as these transient failures
      occur during the course of normal operation of a distributed system.
     */
    properties.put(ProducerConfig.RETRIES_CONFIG, 3); /* Resend in case of transient failure */
    properties.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 1); /* Prevent re-ordering of records during retries */
    properties.put(ProducerConfig.ACKS_CONFIG, "all"); /* Require all in-sync replicas to ack */

    /* SerDes */
    properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

    producer = new KafkaProducer<>(properties);
    LOG.info("started");
  }

  public Future<RecordMetadata> send(String message) {
    return producer.send(new ProducerRecord<>(config.getTopic(), message, message));
  }

  public void stop() throws Exception {
    LOG.info("stopping");
    Producer<String, String> producer = this.producer;
    this.producer = null;
    LOG.info("closing producer");
    producer.close();
    LOG.info("stopped");
  }
}
