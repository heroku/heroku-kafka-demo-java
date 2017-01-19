package com.heroku.kafka.demo;

import io.dropwizard.Application;
import io.dropwizard.assets.AssetsBundle;
import io.dropwizard.configuration.EnvironmentVariableSubstitutor;
import io.dropwizard.configuration.SubstitutingSourceProvider;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;
import org.glassfish.jersey.linking.DeclarativeLinkingFeature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DemoApplication extends Application<DemoConfiguration>  {
  private static final Logger LOG = LoggerFactory.getLogger(DemoApplication.class);

  public static void main(String[] args) throws Exception {
    new DemoApplication().run(args);
  }

  @Override
  public String getName() {
    return "heroku-kafka-demo";
  }

  @Override
  public void initialize(Bootstrap<DemoConfiguration> bootstrap) {
    bootstrap.addBundle(new AssetsBundle("/assets/index.html", "/"));
    bootstrap.setConfigurationSourceProvider(
            new SubstitutingSourceProvider(bootstrap.getConfigurationSourceProvider(),
                    new EnvironmentVariableSubstitutor()
            )
    );
  }

  @Override
  public void run(DemoConfiguration config, Environment env) throws Exception {
    DemoProducer producer = new DemoProducer(config.getKafkaConfig());
    DemoConsumer consumer = new DemoConsumer(config.getKafkaConfig());

    env.lifecycle().manage(producer);
    env.lifecycle().manage(consumer);

    env.jersey().register(DeclarativeLinkingFeature.class);
    env.jersey().register(new DemoResource(producer, consumer));
  }
}