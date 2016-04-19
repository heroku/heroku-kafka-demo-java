package com.heroku.kafka.demo;

import com.loginbox.heroku.config.HerokuConfiguration;

public class DemoConfiguration extends HerokuConfiguration {
  private final KafkaConfig kafkaConfig = new KafkaConfig();

  public KafkaConfig getKafkaConfig() {
    return kafkaConfig;
  }
}
