package com.heroku.kafka.demo;

import javax.validation.Valid;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.dropwizard.Configuration;

public class DemoConfiguration extends Configuration {

  @Valid
  @JsonProperty("kafka")
  private final KafkaConfig kafkaConfig = new KafkaConfig();

  public KafkaConfig getKafkaConfig() {
    return kafkaConfig;
  }
}
