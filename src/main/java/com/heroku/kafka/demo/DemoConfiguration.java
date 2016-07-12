package com.heroku.kafka.demo;

import com.loginbox.heroku.config.HerokuConfiguration;

import javax.validation.Valid;

public class DemoConfiguration extends HerokuConfiguration {

  @Valid
  private final KafkaConfig kafkaConfig = new KafkaConfig();

  public KafkaConfig getKafkaConfig() {
    return kafkaConfig;
  }
}
